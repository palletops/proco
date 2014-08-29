(ns proco.app
  (:require [cheshire.core :as json]
            [clj-http.client :as client]
            [clojure.core.async :as async :refer [>!! <!! chan sliding-buffer alts!!]]
            [compojure.core :as server]
            [liberator.core :as rest]
            [ring.middleware.params :refer [wrap-params]]
            [taoensso.timbre :as log]
            [liberator.dev :refer [wrap-trace]]
            [com.palletops.leaven :as leaven]
            [com.palletops.leaven.protocols :refer [start stop]]
            [com.palletops.bakery.jetty :as jetty]))


;; todos:
;; store timestamps for significant events for later metrics
(def ^:dynamic *config*
  "The options that define the runtime characteristics of this service"
  {:incoming-jobs-size 2
   :status-chan-size 25
   :scheduled-tasks-size 5
   :finished-tasks-size 5
   :job-to-tasks-threads 1
   :initial-job-id 1
   :initial-task-id 1})

(defn get-config [c-map k]
  (or (k c-map)
      (or (k *config*)
          :unspecified)))

(defn timestamp [] (System/currentTimeMillis))


(defn queue-job
  "Adds a job to the incoming queue.

  If the queue is full it will return a map with
  `{::service-not-available true}}`, signaling that the `incoming-jobs`
  channel is full. The job qill not be posted in the channel.

  If there is room in the `incoming-jobs` channel, then the job will
  be posted there and the job `:id` returned in a map"
  [incoming-jobs job-id-chan job]
  (log/debug (format "queueing: %s" job))
  (let [ ;; get the next job id. These ids are local to the service.
        ts (timestamp)
        id (async/<!! job-id-chan)
        _ (log/debugf "assigned job id: %s" id)
        ;; augment the job map with id and a status channel to
        ;; report events
        updated-job (assoc job
                      :id id
                      ;;:status-chan status-chan
                      :ts-incoming-queue ts)
        ;; either add the new job to the incoming queue, or return a
        ;; fail value if the queue can't take it
        ;; _ (log/debug (format "Updated job: %s" updated-job))
        [val ch] (try (alts!!
                       ;; add new job to queue
                       [[incoming-jobs updated-job]]
                       ;; return :incoming-queue-full if the post fails
                       :default ::incoming-queue-full)
                      (catch Throwable t
                        (log/error "Error adding job to incoming queue: " t)))]
    ;; (log/debug (format "val: %s" val))
    ;; check if the post succeded, if it didn't, return a flag
    ;; indicating that the service is currently not available
    (condp = val
      ::incoming-queue-full
      (do
        (log/warn "Queue is full. Not accepting request id:" id)
        {::service-not-available true
         :x-status-detail "job queue full "})
      true {:job-id id}
      :default true)))

(defn run-job
  "Requests the service to run a job request as it arrives from
  liberator (in the form of a POST)`"
  [incoming-jobs job-id-chan]
  (fn [ctx]
    ;;(log/debug (with-out-str (clojure.pprint/pprint ctx)))
    (let [data-ctx (-> ctx
                       (dissoc :representation :resource)
                       (assoc-in [:request :body] (slurp (get-in ctx [:request :body]))))]
      ;;(log/debug (str "data:\n" (with-out-str (clojure.pprint/pprint data-ctx))))
      (queue-job incoming-jobs
                 job-id-chan
                 (json/parse-string (get-in data-ctx [:request :body]) true)))))


;; The job REST resource definition for Liberator
(defn job [incoming-jobs-chan job-id-chan]
  (rest/resource 
   :allowed-methods [:post]
   :available-media-types ["application/json"]
   :handle-ok (fn [_] {:status 200})
   :handle-created
   ;; we need to check if the `post!` did actually work, and we do so
   ;; by checking for `{::service-not-available true}`. If not
   ;; available, we need to return a 503 error (instead of a 201 or
   ;; whatever is the code for a failed post)
   (fn [ctx]
     (log/debugf "status: %s job-id: %s x-status-detail: %s"
                 (:status ctx) (:id ctx)
                 (:x-status-detail ctx))
     ;; if the post! failed becuase the queue is full, then we need to
     ;; send back a 503, overriding the default return code. Hence the
     ;; use of `ring-response`
     (if (::service-not-available ctx)
       (liberator.representation/ring-response {:status 503 :body "Server busy."})
       ;; otherwise we return the job id
       (select-keys ctx [:id :x-status-detail])))
   :post! (run-job incoming-jobs-chan job-id-chan)))

;; server definition
(defn app [incoming-jobs-chan job-id-chan]
  (server/routes
   (server/ANY "/job" [] (job incoming-jobs-chan job-id-chan))))

(defn handler [incoming-jobs-chan job-id-chan] 
  (-> (app incoming-jobs-chan job-id-chan) 
      (wrap-params)))

;; client
(defn submit-job [cm port job]
  (try (client/post (format "http://localhost:%s/job" port)
                    {:accept :json
                     :content-type :json
                     :body (json/generate-string job)
                     :connection-manager cm})
       (catch Exception e
         (println "Got a 503 ")
         e)))

(defn submit-test [cm port job]
  (let [payload (submit-job cm port job)]
    (log/debug "received response" payload)
    (json/parse-string (:body payload) true)))


(defn submit-tests
  [port jobs & [conf]]
  (let [cm-conf (merge {:timeout 100 :threads 25} conf)
        cm (clj-http.conn-mgr/make-reusable-conn-manager cm-conf)]
    (doseq [j jobs]
      (submit-test cm port j))))

;;; job-to-task-processor
(defn job-processor [task-id-chan]
  (flatmap
   (fn [{:keys [id tasks]}]
     (log/debugf "job-processor: job-id: %s #tasks: %s" id (count tasks))
     (let [ret
           (map #(let [task (merge % {:job-id id
                                      :id (<!! task-id-chan)
                                      :ts-created (timestamp)})]
                   (log/debugf "enqueuing task id: %s from job: %s" (:id task) id)
                   task)
                tasks)]
       ;; (log/debugf "job-processor ret: %s" ret)
       ret))))


(defn exhndlr [e]
  (log/error "job-to-task-pipeline" e))


;;;; Stuff for testing
(defn chan-drain
  "Builds a go process to drain a channel"
  [c]
  (let [ctr (async/chan)]
    (async/go-loop []
      (let [[val port] (async/alts! [c ctr])]
        (when (not (= port ctr))
          (let [o val
                o (assoc o :ts-exit (timestamp))
                duration (- (:ts-exit o)
                            (:ts-created o))
                o (assoc o :duration duration)]
            (log/infof "Output: %s" o)
            (recur)))))
    ctr))

(defn build-job
  "Builds a job with id and n tasks"
  [id n]
  {:id id
   :tasks
   (for [t (range n)]
     {:do (format "job %s: task %s of %s" id t n)})})

(defn build-jobs [initial-id n-jobs n-tasks-per-job]
  (for [n (range n-jobs)]
    (build-job (+ initial-id n) n-tasks-per-job)))

(defn id-chan
  "Builds a process that produces and pushes to a channel the sequence
  of naturals starting from n"
  ([c] (id-chan c 1))
  ([c n]
     (async/go-loop [i n]
       (log/debugf "curr id: %s" i)
       (async/>! c i)
       (recur (inc i)))))

;; A proco component is a pipeline (but not an async/pipeline, yet) that takes an input channel (incoming-jobs-chan), an output channel (finished-task-chan) and a configuration map. It builds 
(defrecord ProcoComponent [config incoming-jobs-chan finished-tasks-chan]
  com.palletops.leaven.protocols/ILifecycle
  (start [{:keys [conf] :as component}]
    (let [scheduled-tasks-buffer (async/buffer (get-config config :scheduled-tasks-size))
          scheduled-tasks-chan (chan scheduled-tasks-buffer)]
      (assoc component
        :state
        {:finished-tasks-chan finished-tasks-chan
         :scheduled-tasks-buffer scheduled-tasks-buffer
         :scheduled-tasks-chan scheduled-tasks-chan
         :incoming-jobs-chan incoming-jobs-chan
         :jobs-to-task
         (let [ic (chan)]
           (id-chan ic (get-config config :initial-task-id) )
           (async/pipeline
            (get-config conf :job-to-tasks-threads)
            finished-tasks-chan
            (job-processor ic)
            incoming-jobs-chan
            false ;; don't shut it down if input is closed
            exhndlr))}))))

(defn proco-component
  [conf incoming-jobs-chan finished-tasks-chan]
  (map->ProcoComponent
   {:conf conf
    :incoming-jobs-chan incoming-jobs-chan
    :finished-tasks-chan finished-tasks-chan}))

;; jetty component
(leaven/defsystem Proco [:processor :http-server])

(defn proco [config finished-tasks-chan]
  (let [{:keys [http-config processor-config]
         :or {http-config {:port 3000 :join? false}}}
        config
        ;; the job-id-chan is external to both services because 
        job-id-chan (chan)
        ;; start an ID process in the channel
        _ (id-chan job-id-chan (get-config processor-config :initial-job-id))
        incoming-jobs-chan (chan (get-config processor-config :incoming-jobs-size))
        processor (proco-component processor-config incoming-jobs-chan finished-tasks-chan)
        ring-handler (handler incoming-jobs-chan job-id-chan)]
    (log/debugf "Ring handler: %s" ring-handler)
    (map->Proco
     {:http-server (jetty/jetty
                    {:config http-config
                     :handler ring-handler} )
      :processor processor})))

(defn test-starter [port & [drain?]]
  (let [output-chan (async/chan 10)
        component
        {:output-chan output-chan
         :component
         (start
          (proco {:http-config {:port port}} output-chan))}]
    (if drain?
      (assoc component :drain-ctrl (chan-drain output-chan))
      component)))

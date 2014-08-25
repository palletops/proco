(ns proco.app
  (:require [cheshire.core :as json]
            [clj-http.client :as client]
            [clojure.core.async :as async :refer [>!! <!! chan sliding-buffer alts!!]]
            [compojure.core :as server]
            [liberator.core :as rest]
            [ring.adapter.jetty :refer [run-jetty]]
            [ring.middleware.params :refer [wrap-params]]
            [taoensso.timbre :as log]
            [liberator.dev :refer [wrap-trace]]))


;; todos:
;; store timestamps for significant events for later metrics
(def ^:dynamic *config*
  "The options that define the runtime characteristics of this service"
  {:incoming-jobs-size 2
   :status-chan-size 25
   :scheduled-tasks-size 5
   :finished-tasks-size 5})

(defn get-config
  "Helper to get config parameters"
  [k]
  (get *config* k :config-not-found))

(def last-job-id (ref 0))
(defn next-job-id []
  (dosync (alter last-job-id inc) @last-job-id))

(def last-task-id (ref 0))
(defn next-task-id []
    (dosync (alter last-task-id inc) @last-task-id))

(def in-flight-jobs (ref []))
(def finished-jobs (ref []))

(def finished-tasks-buffer
  (async/buffer (get-config :finished-tasks-size)))

(def finished-tasks (chan finished-tasks-buffer))

(def scheduled-tasks-buffer
  (async/buffer (get-config :scheduled-tasks-size)))

(def scheduled-tasks (chan scheduled-tasks-buffer))

;; Buffer counterpart to `incoming-jobs`
(def incoming-jobs-buffer
  (async/buffer (get-config :incoming-jobs-size )))

;; A Channel that holds the jobs that have just arrived but haven't
;; been processed. Only the most basic onboarding has been done
(def incoming-jobs
  (chan incoming-jobs-buffer))

(defn timestamp [] (System/currentTimeMillis))

(defn queue-job
  "Adds a job to the incoming queue.

  If the queue is full it will return a map with
  `{::service-not-available true}}`, signaling that the `incoming-jobs`
  channel is full. The job qill not be posted in the channel.

  If there is room in the `incoming-jobs` channel, then the job will
  be posted there and the job `:id` returned in a map"
  [job]
  (log/debug (format "queueing: %s" job))
  (let [status-chan (-> :status-chan-size get-config sliding-buffer chan)]
    (let [ ;; get the next job id. These ids are local to the service.
          ts (timestamp)
          id (dosync (alter last-job-id inc) @last-job-id)
          ;; augment the job map with id and a status channel to
          ;; report events
          updated-job (assoc job
                        :id id
                        :status-chan status-chan
                        :ts-incoming-queue ts)
          ;; either add the new job to the incoming queue, or return a
          ;; fail value if the queue can't take it
          _ (log/debug (format "Updated job: %s" updated-job))
          [val ch] (alts!!
                    ;; add new job to queue
                    [[incoming-jobs updated-job]]
                    ;; return :incoming-queue-full if the post fails
                    :default ::incoming-queue-full)]
      (log/debug "here!")
      (log/debug (format "val: %s" val))
      ;; check if the post succeded, if it didn't, return a flag
      ;; indicating that the service is currently not available
      (condp = val
        ::incoming-queue-full
        (do
          (log/warn "Queue is full. Not accepting request id:" id)
          {::service-not-available true
           :x-status-detail "job queue full "})
        true {:job-id id}
        :default true))))

(defn run-job
  "Requests the service to run a job request as it arrives from
  liberator (in the form of a POST)`"
  [ctx]
  ;;(log/debug (with-out-str (clojure.pprint/pprint ctx)))
  (let [data-ctx (-> ctx
                     (dissoc :representation :resource)
                     (assoc-in [:request :body] (slurp (get-in ctx [:request :body]))))]
    ;;(log/debug (str "data:\n" (with-out-str (clojure.pprint/pprint data-ctx))))
    (queue-job (json/parse-string (get-in data-ctx [:request :body]) true))))


;; The job REST resource definition for Liberator
(rest/defresource job
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
  :post! run-job)

;; server definition
(server/defroutes app
  (server/ANY "/job" [] job))

(def handler 
  (-> app 
      (wrap-params)))


;; server runtime
(defonce server (run-jetty #'handler {:port 3000 :join? false}))
(defn stop-server [] (.stop server))
(defn start-server [] (.start server))


;; client
(defn submit-job [job]
  (client/post "http://localhost:3000/job"
               {:accept :json
                :content-type :json
                :body (json/generate-string job)}))

(defn submit-test [job]
  (let [payload (submit-job job)]
    (log/debug "received response" payload)
    (json/parse-string (:body payload) true)))


;;;; TEST REDUCER
(def job-processor
  (flatmap
   (fn [{:keys [id tasks]}]
     (log/debugf "job-processor: job-id: %s #tasks: %s" id (count tasks))
     (let [ret
           (into []
                 (map #(let [task (merge % {:job-id id
                                            :id (next-task-id)
                                            :ts-created (timestamp)})]
                         (log/debugf "enqueuing task id: %s from job: %s" (:id task) id)
                         task)
                      tasks))]
       (log/debugf "job-processor ret: %s" ret)
       ret))))

(def out (chan))

(defn exhndlr [e]
  (log/error "job-to-task-pipeline" e))

(def job-to-task-pipeline 
  (async/pipeline 1 scheduled-tasks job-processor incoming-jobs false exhndlr))

(defn get-output []
  (<!! scheduled-tasks))

(defonce drain? (atom true))
(defn output-drainer []
  (let [t (Thread. (fn []
                     (async/go
                       (while @drain?
                         (let [o (async/<! scheduled-tasks)
                               o (assoc o :ts-exit (timestamp))
                               duration (- (:ts-exit o)
                                           (:ts-created o))
                               o (assoc o :duration duration)]
                           (log/infof "Output: %s" o))))))]
    (.start t)
    t))

(defn build-job [id n]
  {:id id
   :tasks
   (for [t (range n)]
     {:do (format "job %s: task %s of %s" id t n)})})

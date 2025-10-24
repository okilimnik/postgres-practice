(ns okilimnik.postgres.basics
  (:refer-clojure :exclude [update set])
  (:require
   [okilimnik.postgres.json :refer [jsonb]]
   [next.jdbc :as jdbc]
   [honey.sql :as sql]))

(def datasource (jdbc/get-datasource {:dbtype "postgres" :dbname "mydb" :username "practitioner" :password "practice"}))
(def conn (jdbc/get-connection datasource))
(jdbc/execute! conn (sql/format {:drop-table [:if-exists :products]}))
(jdbc/execute! conn (sql/format {:drop-table [:if-exists :orders]}))
(jdbc/execute! conn (sql/format {:drop-table [:if-exists :users]}))


;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Topic 1: Create Table ;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;

;; Create Table
(let [q (sql/format {:create-table [:users :if-not-exists]
                     :with-columns [[:id :serial :primary-key]
                                    [:name [:varchar 100] :not-null]
                                    [:email [:varchar 100] :unique]
                                    [:age :int]]})]
  (print q)
  (jdbc/execute! conn q))

;; Create Table with Foreign Key
(let [q (sql/format {:create-table [:orders :if-not-exists]
                     :with-columns [[:id :serial :primary-key]
                                    [:user-id :int :references [:users :id]]
                                    [:product :jsonb]
                                    [:created-at :timestamp :default [:now]]]})]
  (print q)
  (jdbc/execute! conn q))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Topic 2: INSERT Data (DML) ;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

;; Insert a Single Row
(let [q (sql/format {:insert-into :users
                     :values [{:name "Alice"
                               :email "alice@email.com"
                               :age 25}]})]
  (print q)
  (jdbc/execute! conn q))

;; Insert Multiple Rows
(let [q (sql/format {:insert-into :users
                     :values [{:name "Bob"
                               :email "bob@email.com"
                               :age 30}
                              {:name "Clara"
                               :email "clara@email.com"
                               :age 22}]})]
  (print q)
  (jdbc/execute! conn q))

;; Insert with Defaults
(let [q (sql/format {:insert-into :orders
                     :values [{:user-id 1
                               :product (jsonb {:name "Laptop" :price 1200})}]
                     :returning [:id :product]})]
  (print q)
  (jdbc/execute! conn q))

;; Insert with RETURNING
(let [q (sql/format {:insert-into :users
                     :values [{:name "Diana" :email "diana@email.com" :age 28}]
                     :returning [:id :name]})]
  (print q)
  (jdbc/execute! conn q))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Topic 3: SELECT Queries ;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

;; Select All Rows
(let [q (sql/format {:select [:*] :from :users})]
  (print q)
  (jdbc/execute! conn q))

;; Select Specific Columns
(let [q (sql/format {:select [:name :age] :from :users})]
  (print q)
  (jdbc/execute! conn q))

;; Filtering with WHERE
(let [q (sql/format {:select [:name :age]
                     :from :users
                     :where [:> :age 25]})]
  (print q)
  (jdbc/execute! conn q))

;; ORDER BY and LIMIT
(let [q (sql/format {:select [:*]
                     :from :users
                     :order-by [[:age :desc]]
                     :limit 5})]
  (print q)
  (jdbc/execute! conn q))

;; Aliases
(let [q (sql/format {:select [[:u.name :username] :u.age]
                     :from [[:users :u]]})]
  (print q)
  (jdbc/execute! conn q))

;; Conditions with AND / OR
(let [q (sql/format {:select [:*]
                     :from [:users]
                     :where [:and
                             [:> :age 18]
                             [:is-not :email nil]]})]
  (print q)
  (jdbc/execute! conn q))


;;;;;;;;;;;;;;;;;;;;
;; Topic 4: JOINs ;;
;;;;;;;;;;;;;;;;;;;;

;; INNER JOIN
(let [q (sql/format {:select [:u.name :o.product]
                     :from [[:users :u]]
                     :join [[:orders :o] [:= :u.id :o.user-id]]})]
  (print q)
  (jdbc/execute! conn q))

;; LEFT JOIN
(let [q (sql/format {:select [:u.name :o.product]
                     :from [[:users :u]]
                     :left-join [[:orders :o] [:= :u.id :o.user-id]]})]
  (print q)
  (jdbc/execute! conn q))

;; RIGHT JOIN
(let [q (sql/format {:select [:u.name :o.product]
                     :from [[:users :u]]
                     :right-join [[:orders :o] [:= :u.id :o.user-id]]})]
  (print q)
  (jdbc/execute! conn q))

;; FULL OUTER JOIN
(let [q (sql/format {:select [:u.name :o.product]
                     :from [[:users :u]]
                     :full-join [[:orders :o] [:= :u.id :o.user-id]]})]
  (print q)
  (jdbc/execute! conn q))

;; Joining Multiple Tables (pay attention, product is a JSONB field)
(let [q (sql/format {:create-table [:products :if-not-exists]
                     :with-columns [[:id :serial :primary-key]
                                    [:name [:varchar 100] :not-null]
                                    [:category [:varchar 100] :not-null]]})]
  (jdbc/execute! conn q))
(let [q (sql/format {:insert-into :products
                     :values [{:name "Laptop"
                               :category "Computers"}]})]
  (jdbc/execute! conn q))
(let [q (sql/format {:select [:u.name :o.product :p.category]
                     :from [[:users :u]]
                     :join [[:orders :o] [:= :u.id :o.user-id]
                            [:products :p] [:= [:raw "(o.product ->> 'name')"] :p.name]]})]
  (print q)
  (jdbc/execute! conn q))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Topic 5: Aggregations & GROUP BY ;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

;; COUNT
(let [q (sql/format {:select [[[:count :*]]]
                     :from [:users]})]
  (print q)
  (jdbc/execute! conn q))

(let [q (sql/format {:select [:%count.*]
                     :from [:users]})]
  (print q)
  (jdbc/execute! conn q))

;; SUM, AVG, MIN, MAX
(let [q (sql/format {:select [:%sum.age :%avg.age :%min.age :%max.age]
                     :from [:users]})]
  (print q)
  (jdbc/execute! conn q))

;; GROUP BY
(let [q (sql/format {:select [:age :%count.*]
                     :from [:users]
                     :group-by [:age]})]
  (print q)
  (jdbc/execute! conn q))

;; HAVING (Filter Groups)
(let [q (sql/format {:select [:age :%count.*]
                     :from [:users]
                     :group-by [:age]
                     :having [:> :%count.* 1]})]
  (print q)
  (jdbc/execute! conn q))

;; Aggregation with JOIN
(let [q (sql/format {:select [:u.name [[:count :o.id] :total-orders]]
                     :from [[:users :u]]
                     :left-join [[:orders :o] [:= :u.id :o.user-id]]
                     :group-by [:u.name]})]
  (print q)
  (jdbc/execute! conn q))

;; Multiple Aggregates & GROUP BY
(let [q (sql/format {:select [:u.name [[:count :o.id] :total-orders]
                              [[:avg [:raw "(o.product->>'price')::numeric"]]
                               :avg-price]]
                     :from [[:users :u]]
                     :left-join [[:orders :o] [:= :u.id :o.user-id]]
                     :group-by [:u.name]})]
  (print q)
  (jdbc/execute! conn q))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Topic 6: Window Functions ;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

;; RANK()
(let [q (sql/format {:select [:name :age [[:over [[:rank] {:order-by [[:age :desc]]}]]
                                          :age-rank]]
                     :from [:users]})]
  (print q)
  (jdbc/execute! conn q))

;; ROW_NUMBER()
(let [q (sql/format {:select [:name [[:over [[:row_number] {:order-by [:id]}]]
                                     :row-num]]
                     :from [:users]})]
  (print q)
  (jdbc/execute! conn q))

;; PARTITION BY
(let [q (sql/format {:select [:user-id [[:over [[:count :*] {:partition-by [:user-id]}]]
                                        :orders-per-user]]
                     :from [:orders]})]
  (print q)
  (jdbc/execute! conn q))

;; SUM() OVER (Running Total)
(let [q (sql/format {:select [:user-id :id [[:over [[:sum :id] {:partition-by [:user-id]
                                                                :order-by [:id]}]]
                                            :running-total]]
                     :from [:orders]})]
  (print q)
  (jdbc/execute! conn q))

;; Moving Average
(let [q (sql/format {:select [:user-id :id [[:raw "AVG(id) OVER (PARTITION BY user_id ORDER BY id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)"]
                                            :moving-avg]]
                     :from [:orders]})]
  (print q)
  (jdbc/execute! conn q))
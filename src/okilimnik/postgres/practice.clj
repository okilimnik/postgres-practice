(ns okilimnik.postgres.practice
  (:refer-clojure :exclude [update set])
  (:require
   [okilimnik.postgres.practice.json :refer [jsonb]]
   [next.jdbc :as jdbc]
   [honey.sql :as sql]))

(def datasource (jdbc/get-datasource {:dbtype "postgres" :dbname "mydb" :username "practitioner" :password "practice"}))
(def conn (jdbc/get-connection datasource))

;; Drop Tables
(let [q (sql/format {:drop-table :orders})]
  (print q)
  (jdbc/execute! conn q))
(let [q (sql/format {:drop-table :users})]
  (print q)
  (jdbc/execute! conn q))

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


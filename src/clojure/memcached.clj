(ns clojure.memcached
  (:import [java.net Socket ServerSocket UnknownHostException]
	   [java.io IOException DataInputStream DataOutputStream]
	   [java.util.regex Pattern]
	   [java.security NoSuchAlgorithmException MessageDigest]))

(defn- md5 
  "Return md5 hash string"
  [str]
  (let [alg (doto (MessageDigest/getInstance "MD5")
	      (.reset)
	      (.update (.getBytes str)))]
    (try
     (.toString (new BigInteger 1 (.digest alg)) 16)
     (catch NoSuchAlgorithmException e
       (throw (new RuntimeException e))))))

(defn- break [s n]
  (loop [i 0, result ""]
    (if (= i n) 
      result
      (recur (inc i) (str result (get s i))))))

(defn- split
  ([#^String s #^Pattern re] (seq (.split re s)))
  ([#^String s #^Pattern re limit] (seq (.split re s limit))))

(defn which-server? 
  "Takes the md5 of the key, breaks it into a 10 character string,
   turns it into long value, then returns the long value mod n. 
   The purpose is to randomly distribute data among the server pool"
  [sockets key]
  (int (mod (. Long (valueOf (break (md5 key) 10) 16)) (count sockets))))

(defn setup-sockets 
  "Initializes memcached. vec is a vector with host:port values."
  [a]
  (loop [arr a, result []]
    (if (= nil (first arr)) 
      result
      (let [server (first arr)
	    sarr (split server #":")
	    port (if (= (count sarr) 2)
		   (first (rest sarr))
		   "80")
	    host (first sarr)
	    client (new Socket host (new Integer port))]
	(recur (rest arr) (conj result client))))))

(defn close-sockets 
  "Run this to close any sockets that are open."
  [sockets]
  (loop [s sockets]
    (if (= nil (first s))
      true
      (let [socket (first s)]
	(. socket close)
	(recur (rest s))))))

(defn- init [sockets key f]
  (try
   (let [client (sockets (which-server? sockets key))
	 os (new DataOutputStream (. client getOutputStream))
	 is (new DataInputStream (. client getInputStream))]
     (f os is))
   (catch UnknownHostException e (println e))
   (catch IOException e (println e))))


(defn set-val 
  "Sets a value in memcached. If a value already exists with that key, it
   will be replaced."
  [sockets key val]
  (init 
   sockets key
   (fn [os is]
     (. os (writeBytes (str "set " key " 0 0 " (count val) "\r\n" val "\r\n")))
     (loop []
       (let [response (. is readLine)]
	 (cond (= nil response) false
	       (not= (. response trim) "STORED") (recur)
	       :else true))))))

(defn delete-val [sockets key]
  (init sockets key
	(fn [os is]
	  (. os (writeBytes (str "delete " key "\r\n")))
	  (let [response (. (. is readLine) trim)]
	    response))))

(defn get-val 
  "Gets the key's value in memcached."
  [sockets key]
  (init 
   sockets key
   (fn [os is]
     (. os (writeBytes (str "get " key " \r\n")))
     (let [r (loop [full-response '()]
	       (let [response (. is readLine)]
		 (cond (= nil response) ""
		       (= "END" (. response trim)) (conj full-response response)
		       (not= "END" (. response trim)) (recur (conj full-response response)))))]
       (if (= (count r) 1)
	 nil
	 (first (rest r)))))))




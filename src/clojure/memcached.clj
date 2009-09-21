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
   turns it into long value, then returns that value mod n. 
   The purpose is randomly distribute among the server pool based 
   on the keys."
  [key n]
  (int (mod (. Long (valueOf (break (md5 key) 10) 16)) n)))

(defn- setup-sockets [a]
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

(defn setup-memcached 
  "Initializes memcached. vec is a vector with host:port values."
  [vec]
  (def sockets (ref (setup-sockets vec))))

(defn- close-sockets []
  (loop [s @sockets]
    (if (= nil (first s))
      true
      (let [socket (first s)]
	(. socket close)
	(recur (rest s))))))

(defn close-memcached 
  "Run this to close any sockets that are open."
  []
  (close-sockets))

(defn- init [key f]
  (try
   (let [client (@sockets (which-server? key (count @sockets)))
	 os (new DataOutputStream (. client getOutputStream))
	 is (new DataInputStream (. client getInputStream))]
     (f os is))
   (catch UnknownHostException e e)
   (catch IOException e e)))

(defn set-val 
  "Sets a value in memcached. If a value already exists with that key, it
   will be replaced."
  [key val]
  (init 
   key
   (fn [os is]
     (. os (writeBytes (str "set " key " 0 0 " (count val) "\r\n" val "\r\n")))
     (loop []
       (let [response (. is readLine)]
	 (cond (= nil response) false
	       (not= (. response trim) "STORED") (recur)
	       :else true))))))

(defn get-val 
  "Gets the key's value in memcached."
  [key]
  (init 
   key
   (fn [os is]
     (. os (writeBytes (str "get " key " \r\n")))
     (let [r (loop [full-response '()]
	       (let [response (. is readLine)]
		 (cond (= nil response) ""
		       (= "END" (. response trim)) (conj full-response response)
		       (not= "END" (. response trim)) (recur (conj full-response response)))))]
       (first (rest r))))))

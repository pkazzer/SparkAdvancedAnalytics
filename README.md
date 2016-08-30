



# SparkAdvancedAnalytics

start shell using putty
server: cdhsync-mn0.northeurope.cloudapp.azure.com

spark-shell --jars /home/cloudera/AdvancedAnalytics/ch06-lsa/target/ch06-lsa-1.0.2-jar-with-dependencies.jar  --num-executors 11

run code in uebung_spark.scala


tfidf, kmeans, SVD, Hive&amp;HBase

Kmeans
+ Vector.sparse Aufbau: (Länge des Vektors, nonzero TermIds, values)
+Output sind clusterIds (und features) siehe kmeans: 












http://192.168.1.243:8888/filebrowser/view=/user/DemoUser/kmeans/part-00000
+Matching mit Unique ID, um als Ergebnis Cluster Id, Tweettext und Id_str zu bekommen
-Eigentlich wird als Input ein Vector.dense erwartet, stimmt das Clustering dann noch?
-Clustering für K=20 und K=60: Tweets haben minimale Ähnlichkeit. Clusterstruktur ist nicht erkennbar. (http://192.168.1.243:8888/filebrowser/view=/user/DemoUser/win/_temporary/0/_temporary/attempt_201608031637_0529_m_000000_334/part-00000) -Woran liegt das? –schlechte Tweets? –Vector.sparse? –Matching mit Unique Id? 
-Fehler beim Matching: java.utilNoSuchElementException: key not found 4959
-Bei k=100 Java Heap Space Exception

SVD
+Code lässt sich umsetzen
-Clusterstruktur ist schwer zu erkennen. Gründe:  -preprocessing? –Daten?

Betrachten der Ergebnisse
Dataframes:
Eg. Hivesql.explain(),
hivesql.show(3) zeigt drei ersten zeilen der Tabelle
RDD[Row]
Eg. rdd.first erste Zeile
rdd.first.getLong(1)/getString(0) Long an Position 1/String an Position 2
rdd.take(2) erste zwei Zeilen
RDD[String, Long]
Eg. raw.first._1/_2 Erste Zeile, String/Long anzeigen

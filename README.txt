Gil Yadgar – 311334825 (gilyad@post.bgu.ac.il)
Niv Zetuni – 307852897 (nivzet@post.bgu.ac.il)

How to run:
	1) Fill the bucket directory parameter.
	2) Make sure there are no "output1", "output2", "output3" folders in this bucket.
	3) change the aggregation parameter to false if you dont want to active the combiner.
	4) make sure you field your key paramter right.
	5) run the main program and wait for the output in your bucket. 


We have 6 classes in our project:

Main.java:
	•This is the main class that the client interacts with, it got all the technical functions to communicate with AWS and create a new EMR "job flow" with three steps, then shutdown.

Stripe.java:
	•Extend the "MapWritable" class.
	•Have the "add" function that combine stripes (if they have the same key, then we sum the values together).

Value.java:
	•The purpose of this class is to represent the data that we need in the first step for each key.
	•Implements the "Writable" class.
	•Have 3 fields:
		1)LongWritable called "occ" – is the occurrence of the key.
		2)Stripe called "pair" – the keys are the first word of the pair, and the value is the number of the occurrence.
		3)Stripe called "trio" - the keys are the first and the second words of the pair, and the value is the number of the occurrence.
	•Have 5 constructors.

Job1.java:
	•This is the first step of the "job Flow", it receives google 3-gram as an input.
	•Its job is to take from every three word the information which is needed to calculate the probability in job2.
	•The final key-value pairs are:
		1)<*, AllWordsTotalOcc >
		2)<w1 w2, w2TotalOcc w1w2TotalOcc >
		3)<w1 w2 w3, w3TotalOcc w2w3TotalOcc w1w2w3TotalOcc >
	•If "local Aggregation" is true, then we active the combiner, else we only work with the mapper and the reducer. 
	•Mapper: receive "LongWritable" key with "Text" value and emit "Text" key with "Value" value. 
	For each <w1, w2, w3> of words preform the following actions:
		1)Filter all the not valid trio's (that not all the word are Hebrew words, i.e., [".", "בית", "שיעורי"]).
		2)For w1, make Value object called "val1" with the occurrence of the trio in the 3-gram, and emit key-value pair of <w1, val1>.
		3)For w2, make Value object called "val2" with a Stripe map object called "pair" that have the key "w1" with the value of the occurrence of the trio in the 3-gram, and emit key-value pair of <w2, val2>.
		4)For w2, make Value object called "val3" with a Stripe map object called "trio" that have the key "w1 w2" with the value of the occurrence of the trio in the 3-gram, and emit key-value pair of <w3, val3>.
		5)In addition, emit key-value pair of <*, val1>.
	•Combiner: get "Text" key with "Value" value and emit "Text" key with "Value" Value, for every key "wX":
		1)If the key is "*", sum all the occurrences in the value and emit the key-value pair <*, AllWordsTotalOcc>
		2)Else, take all the values of the key and:
			a.Sum all the occurrences.
			b.Merge all the "pair" Stripe maps.
			c.Merge all the "trio" Stripe maps.
		3)Make a new Value object called "newVal" with all the data.
		4)Emit key-value pair of <wX, newVal>
	•Reducer: get "Text" key with "Value" value and emit "Text" key with "Text" Value, for every key "wX":
		1)If the key is "*", sum all the occurrences in the value and emit the key-value pair <*, AllWordsTotalOcc>
		2)Else, take all the values of the key and:
			a.Sum all the occurrences.
			b.Merge all the "pair" Stripe maps.
			c.Merge all the "trio" Stripe maps.
		3)For every key "wY" in the "pair" Stripe map, emit key-value pair of <wY wX, wXTotalOcc wYwXTotalOcc >.
		4)For every key "wY wZ" in the "trio" Stripe map, emit key-value pair of <wY wZ wX, wXTotalOcc wZwXTotalOcc wYwZwXTotalOcc >.

Job2.java:
	•This is the second step of the "job Flow", it receives the output of Job1 as an input.   
	•Its job is to take the information for the probability formula with the parameters that we supply to it from Job1 and calculate it.
	•Sort the keys by w1w2 in an ascending order.
	•Make key-value pairs of:
		1)<w1 w2 w3, P (w3|w1 w2) >
	•Have a unique partitioner class that send all the information of the mappers to the same reducer. In order to make sure that the '*' key's value fill "totalWords" field, and useable to all other keys. 
	•Mapper: get "Text" key with "Text" value and emit "Text" key with "Text" value and:
		1)If the input key has only 1 word or 2 words (that means it have the structure of <*, …> or <w1 w2, …>), it emits to the reducer the same key and value.
		2)Else, if the key has 3 words, it means it from the structure of <w1 w2 w3, w3TotalOcc w2w3TotalOcc w1w2w2TotalOcc>, it emits to the reducer the key-value pair of <w1 w2, w3 w3TotalOcc w2w3TotalOcc w1w2w2TotalOcc> 
	•Reducer: get "Text" key with "Text" value and emit "Text" key with "Text" Value and:
		1)Set up a new parameter called "totalWords".
		2)The first key will be "*" (because of the default sort of the partition comparator), and we save the value of this key in the "totalWords" parameter.
		3)for every key "wX wY": set up 2 new parameters called "c1" and "c2" and a new linked list, take all the values of the key and:
			a.If the value has 2 arguments, then we put the first argument in the "c1" parameter and the second argument in "c2" parameter.
			b.Else, we save the value in the linked list.
		4)Then, for every value in the linked list:
			a.We calculate the probability P (w3 |wX wY) when "N1" is the second argument of the value, "N2" is the third argument of the value and "N3" is the fourth argument of the value.
			b.Take the first argument of the value as "w3" and emit the key-value pair of <wX wY w3, P (w3 |wX wY)>.

Job3.java:
	•This is the third step of the "job Flow", it receives the output of Job2 as an input.   
	•Its job is to take all the key-value pairs, and sort them by the probability for w3 in a descending order.
	•Make key-value pairs of:
		1)<w1 w2 w3, P (w3|w1 w2) >
	•Mapper: get "Text" key with "Text" value and emit "Text" key with "Text" value and:
		1)Take the value of the key (that is the probability) and save in a new parameter called "valueKey" the value of 1-p.
		2)Emit the key-value pair of <w1 w2 valueKey w3, value> 
	•Reducer: get "Text" key with "Text" value and emit "Text" key with "Text" Value and:
		1)for every key "w1 w2 valueKey w3", take the value and emit the key-value pair of <w1 w2 w3, value>.


Statistics:
	*we written the different only between the first steps, because only there a combiner was necessary.
	Without local aggregation:
		Number of key-value pair which were sent from the map to the reduce in step 1: 277762412.
		Their size: 9961114026.
		Map output materialized bytes: 1079889929.
		Arrived at the reducer: 277762412.

	with local aggregation:
		Number of key-value pair which were sent from the map to the reduce in step 1: 277762412.
		Their size: 9961114026.
		Arrived at the combiner:  278860929.
		Sent from the combiner: 1715802.
		Map output materialized bytes: 42160153.	
		Arrived at the reducer: 617285.


Analysis:
	1.The pair: "אל ארץ":
		a.אל ארץ ישראל  0.1754
		b.אל ארץ אחרת	0.0749
		c.אל ארץ טובה	0.0443
		d.אל ארץ יהודה	0.0361
		e.אל ארץ כנען	0.0349
	We were expecting to see Israeli terms and bible terms.

	2.	The pair:  " בית ספר":
		a.בית ספר תיכון	0.0799
		b.בית ספר זה	0.0744
		c.בית ספר עברי	0.0520
		d.בית ספר יסודי	0.0485
		e.בית ספר עממי	0.0369
	It was surprising to see that יסודי is not high as תיכון.

	3.The pair: "רוצה לאכול":
		a.רוצה לאכול את	0.2169
		b.רוצה לאכול משהו 0.1683
		c.רוצה לאכול בשר 0.1608
		d.רוצה לאכול או	0.0801
		e.רוצה לאכול לחם 0.0602
	We were hoping to see something tasty, but apparently no one sure what he wants to eat.

	4.The pair: "אני מרגיש":
		a.אני מרגיש את	0.2599
		b.אני מרגיש כי	0.0433
		c.אני מרגיש עצמי 0.0373
		d.אני מרגיש שאני 0.0801
		e.אני מרגיש כמו	0.0348
	We wanted to check what is the most common feel, we were expecting to see good or bad.

	5.The pair: " אני מאמין":
		a.אני מאמין באמונה 0.2511
		b.אני מאמין לך	0.0496
		c.אני מאמין שיש	0.0308
		d.אני מאמין בכל	0.0304
		e.אני מאמין כי	0.1556
	We were expecting to see the first result higher because of the song.

	6.The pair: " איש לא":
		a.איש לא ידע	0.0935
		b.איש לא היה	0.0742
		c.איש לא יכול	0.0315
		d.איש לא יוכל	0.0263
		e.איש לא ראה	0.0209
	We wanted to see what no man can do.

	7.The pair: " אבא שלי":
		a.אבא שלי היה	0.2736
		b.אבא שלי אמר	0.0780
		c.אבא שלי הוא	0.0635
		d.אבא שלי אומר	0.0460
		e.אבא שלי כבר	0.0234
	We wanted to see what “my father” will give.

	8.The pair: " ריח של":
		a.ריח של בשר	0.0343
		b.ריח של סבון	0.0310
		c.ריח של עשן	0.0299
		d.ריח של בושם	0.0285
		e.ריח של זיעה	0.0278
	We wanted to check what are the most common smells.

	9.The pair: " טעם של":
		a.טעם של ממש	0.1740
		b.טעם של עוד	0.0956
		c.טעם של איסור	0.0839
		d.טעם של יראת	0.0743
		e.טעם של חיים	0.0785
	We wanted to check which taste is the most common.

	10.The pair: " אל תשכח":
		a.אל תשכח את	0.4177
		b.אל תשכח כי	0.0855
		c.אל תשכח לנצח	0.0447
		d.אל תשכח מה	0.0258
		e.אל תשכח ואל	0.0235
	We wanted to check what is important not to forget.



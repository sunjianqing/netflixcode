val t = 5
 (2 to t).map(x => (x, (2 to (t / x))))
val s = (1 to 10)
val res = s.flatMap( x => {
 x match {
 case x if x % 2 == 0 => Some(x)
 case _ => None
 }
})
res.foreach(println(_))






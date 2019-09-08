package com.briup.day4

object TupleTest extends App{
  //1 元组构建    二元元组
  val t1 = new Tuple3[Int,String,Double](1,"jack",75.0)
//  val t2: (Int, String, Double) = (2,"rose",50.0);
  val (id, name, weight) =(2,"rose",50.0);
  val t3 = 10 ->20
  val t4:Unit=()
  //2 元组访问
  println(t1._2)
  println(s"Studnet $id $name $weight")
}

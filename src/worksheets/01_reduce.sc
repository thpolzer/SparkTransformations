/* reduce operations are very similar to Scala fold operations. This worksheets covers the fundamental ideas and syntax
of fold operations
fold operations consecutively combine two neighbouring elements of a list.
fold left starts from the outermost left element, fold right from the last element of a list.
A fold left operation "(z /: xs) (op)" involves three objects:
- a start value z
- a list xs
- and a binary operator op
The result of the fold is op applied between successive elements ofthe list prefixed by z
 */
val list1 = List(1,2,3,4,5,6)
val list2 = List("My","name","is","Thomas")
val list3 = List(6,2,1,15,11)

// 1. fold left
(list1.foldLeft(0)) (_*_)
(list1 foldLeft(0)) (_+_)
(list2 foldLeft("")) (_+" "+_)
(0 /: list1) (_+3+_)

// 2. foldRight
/* for associative operations foldLeft and foldRight are equivalent*/
(list1.foldRight(0)) (_*_)
(list1 foldRight(0)) (_+_)

/*
However pls note: foldLeft and foldRight are not parallelizable and therfore
not supported for RDDs!
 */

// 3. reduce
/* reduce is nearly the same as fold. However, there is no starting element
that need to be passed to the already existing list.
 */
val red_left1 = list1.reduceLeft(_+_)

def calculateMax(x:Int,y:Int):Int = {
  if (x >= y) {
    println (x + " ist größer als "+y)
    x
  } else {
    println (y + " ist größer als "+x)
    y
  }
}

val maxValue1 = list3.reduceLeft((x,y) => calculateMax(x,y))
val maxValue2 = list3.reduce((x,y) => calculateMax(x,y))






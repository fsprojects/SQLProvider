(**
| Keyword            | Supported  |  Notes
| --------------------- |:-:|---------------------------------------|
all	                     |  |                                                       | 
averageBy                |X |                                                       | 
averageByNullable        |  |                                                       | 
contains                |X |                                                       | 
count                    |X |                                                       | 
distinct                 |X |                                                       | 
exactlyOne               |  |                                                       | 
exactlyOneOrDefault      |  |                                                       | 
exists                   |X |                                                       | 
find                     |  |                                                       | 
groupBy                  |  |                                                       | 
groupJoin                |  |                                                       | 
groupValBy	             |  |                                                       | 
head                     |X |                                                       | 
headOrDefault            |  |                                                       | 
if                       |X |                                                       |
join                     |X |                                                       | 
last                     |  |                                                       | 
lastOrDefault            |  |                                                       | 
leftOuterJoin            |  |                                                       | 
let                      |  |                                                       |
maxBy                    |X |                                                       | 
maxByNullable            |  |                                                       | 
minBy                    |X |                                                       | 
minByNullable            |  |                                                       | 
nth                      |  |                                                       | 
select                   |X |                                                       | 
skip                     |X |Broken on SQLServer when combined with sortByDescending + take     | 
skipWhile                |  |                                                       | 
sortBy                   |X |                                                       | 
sortByDescending	       |X |Broken on SQLServer when combined with skip+take       | 
sortByNullable           |  |                                                       | 
sortByNullableDescending |  |                                                       | 
sumBy                    |X |                                                       | 
sumByNullable            |  |                                                       | 
take                     |X |Broken on SQLServer when combined with skip+sortByDescending      | 
takeWhile                |  |                                                       | 
thenBy	                 |X |                                                       |     
thenByDescending	       |  |                                                       |   
thenByNullable           |  |                                                       | 
thenByNullableDescending |  |                                                       |
where                    |X | Server side variables must be on left side and only left side of predicates  | 

*)

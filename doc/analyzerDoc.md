## ApproxCountDistinct
Compute approximated count distinct with [HyperLogLogPlusPlus](https://en.wikipedia.org/wiki/HyperLogLog#HLL++)
## ApproxQuantile
Approximate quantile analyzer. 
Yields the x-quantile; a value, for which it holds true, that exactly x percent of the total values are smaller than the yielded value.
The quantile x can be passed to the analyzer as parameter, x has to be a number between 0 and 1. Choosing 0.5 will yield the median.
The allowed relative error compared to the exact quantile can be configured with `relativeError` parameter. A `relativeError = 0.0` would yield the exact quantile while increasing the computational load.
## ApproxQuantiles
The same as ApproxQuantile, can be given a sequence of quantiles.
## Completeness
Completeness is the fraction of the number of non-null values divided by the [Size](https://github.com/hpi-bp1819-naumann/deequ/blob/documentation/doc/analyzerDoc.md#size) of the column.
## Compliance
Compliance is a measure of the fraction of rows that complies with the given constraint. This constraint may span multiple columns.
e.g Given the constraint is "att1 > 3" and the data frame has 5 rows with an att1 column value greater than
3 and 10 rows with a value lower than 3; a DoubleMetric of 0.33 would be returned.
## Correlation
Computes the [pearson correlation coefficient](https://en.wikipedia.org/wiki/Pearson_correlation_coefficient) between the two given columns.
It returns a value between -1 and +1, where +1 is total positive linear correlation, 0 is no linear correlation, and −1 is total negative linear correlation.
## CountDistinct
Number of distinct values in the column.
## DataType
Yields a distribution map, including the overall number of values of each datatype and the percentage of each datatype. 
Possible datatypes are Boolean, Fractional, Integral and String. If the type can't be determined, Unknown is returned.
## Distinctness
Distinctness is the fraction of the number of distinct values divided by the [Size](https://github.com/hpi-bp1819-naumann/deequ/blob/documentation/doc/analyzerDoc.md#size) of the column. Can be given a sequence of columns.
## Entropy
[Entropy](https://en.wikipedia.org/wiki/Entropy_(information_theory)) is a measure of the level of information contained in a message. Given the probability distribution over values in a column, it describes how many bits are required to identify a value. A highly diverse column will result in high entropy. Few different values will result in low entropy.
## Histogram
Computes the value distribution in the given column. If the user specifies a binning function, bins according to this function are created. Low cardinality can be filtered from the result with the maxDetailBins parameter.
## Maximum
Returns the largest value in the given column. Only for numeric columns.
## Mean
This is the average over all values of the given column. Therefore it is the fraction of the [Sum](https://github.com/hpi-bp1819-naumann/deequ/blob/documentation/doc/analyzerDoc.md#sum) and the [Size](https://github.com/hpi-bp1819-naumann/deequ/blob/documentation/doc/analyzerDoc.md#size) of the column. Only for numeric columns.
## Median
For n rows returns the value with index n/2 in the specified column if sorted. If n is odd the mean between the surrounding indices is computed. This is more robust against outliers than the mean. It is equal to the 2-quantile.
## Minimum
Returns the smallest value in the given column. Only for numeric columns.
## Mode
This is the most common value in the given column. Not necessarily unique.
## MutualInformation
Measure of how much information one column reveals about another. If both columns are independent, the mutual information is zero. If one of the columns determines all values of the other column and the other way around, there is a functional dependency between the columns. In that case, the mutual information is equal to either of both columns' [Entropy](https://github.com/hpi-bp1819-naumann/deequ/blob/documentation/doc/analyzerDoc.md#entropy).
Formally it is the nested sum of all values of both column and then for each pair: p(x,y)log(p(x,y)/p(x)p(y))
## PatternMatch
Gives the fraction of values that match a certain regex constraint divided by all values in the given column.
## Size
Is the amount of values in the given column.
## StandardDeviation
Quantifies the amount of variation of the values in the given column. A low standard deviation means that the values are close to the mean, while a high standard deviation means that they are spread out over a large value range. It is calculated by taking the square root of the variance.
## Sum
The sum of all values in the given column. Only for numeric columns.
## UniqueValueRatio
The quotient of all unique values divided by all distinct values of the given column. The unique values only appear once in the column. The distinct values are all different values in the column where every value is counted once.
## Uniqueness
Gives the fraction of values of the given column that only appear once in the whole column divided by the [Size](https://github.com/hpi-bp1819-naumann/deequ/blob/documentation/doc/analyzerDoc.md#size) of the column.

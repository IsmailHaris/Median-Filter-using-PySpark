# Median Filter using PySpark
Filtering noisy image using a Spark implementation of the median filter. 
Time Comparision with the Python Scipy Implementation. (Speed Up x2)
A Median Filter  consists in replace the value of a pixel p[i,j] by the median value of the list :
[p[i-1,j-1],p[i-1,j],p[i-1,j+1],p[i,j-1],p[i,j],p[i,j+1],p[i+1,j-1],p[i+1,j],p[i+1,j+1]]

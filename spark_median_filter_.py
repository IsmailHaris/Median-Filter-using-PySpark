# command ./bin/spark-submit file path
#path /home/hduser/local/spark
import pyspark
from pyspark import SparkContext
import imageio
import os
import numpy as np



"""
---------------------------------------------------------------------------
MEDIAN FILTER Spark Parallel
---------------------------------------------------------------------------
"""
"""
---------------------------------------------------------------------------
IDEA : To define each pixel, we take the 9 pixels around and take the 
median value.

We replace the value of pixel p[i,j] by the median value of the list :
[p[i-1,j-1], p[i-1,j], p[i-1,j+1],p[i,j-1], p[i,j], p[i,j+1], p[i+1,j-1],
p[i+1,j], p[i+1,j+1]]
----------------------------------------------------------------------------
"""
def readImg(path):
    img = imageio.imread(path)
    im = np.array(img,dtype='uint8')
    return im

def writeImg(path,buf):
    imageio.imwrite(path,buf)

def part_median_filter(local_data):
    part_id = local_data[0]
    begin   = local_data[1]
    end     = local_data[2]
    buf_    = local_data[3]
    nx=buf_.shape[0]
    ny=buf_.shape[1]
    ########################################
    #
    # CREATE NEW BUF WITH MEDIAN FILTER SOLUTION
    #
    new_buf=np.zeros((end-begin+1,ny,3), dtype='uint8')
    buf=buf_[begin:end+1]
    ##########################################
    #
    # TODO COMPUTE MEDIAN FILTER 

    #heart
    for i in range(1, end-begin):
        for j in range(1,ny-1):
            median = np.median((buf[i-1,j-1], buf[i-1,j],buf[i-1,j+1],
                    buf[i,j-1],buf[i,j], buf[i,j+1],
                    buf[i+1,j-1],buf[i+1,j], buf[i+1,j+1]), 
                    axis =0)
            median_int = np.array([(lambda x: int(x))(k) for k in median])
            new_buf[i,j]= median_int
    #border columns 
    for i in range(end-begin+1):
        new_buf[i,0] = buf[i,0]
        new_buf[i,ny-1] = buf[i,ny-1]
    
    #border rows :
    i=0
    for j in range(ny):
        
        temp_buf = []
        temp_index = [[i,j], [i,j+1], [i,j-1],[i+1,j-1],[i+1,j], [i+1,j+1]]

        if j-1<0:
            for element in temp_index:
                if element[1]==j-1:
                    temp_index.remove(element)
        elif j+1>ny-1:
            for element in temp_index:
                if element[1]==j+1:
                    temp_index.remove(element)
        for element in temp_index:
            temp_buf.append (buf[element[0],element[1]])
        median = np.median(temp_buf, axis=0)
        median_int = np.array([(lambda x: int(x))(k) for k in median])
        new_buf[0,j]= median_int
       
    
    i = end-begin
    for j in range(ny):
        
        temp_buf = []
        temp_index = [[i,j], [i,j+1], [i,j-1],
                        [i-1,j-1],[i-1,j], [i-1,j+1]]
        if j-1<0:
            for element in temp_index:
                if element[1]==j-1:
                    temp_index.remove(element)
        elif j+1>ny-1:
            for element in temp_index:
                if element[1]==j+1:
                    temp_index.remove(element)
        for element in temp_index:
            temp_buf.append (buf[element[0],element[1]])
        median = np.median(temp_buf, axis=0)
        median_int = np.array([(lambda x: int(x))(k) for k in median])
        new_buf[end-begin,j]= median_int
    
    return part_id, new_buf
            
    

from pyspark.sql import Row, SQLContext
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import *
import time
from scipy.ndimage.filters import median_filter
def main():
    data_dir = '/home/gext/ismail.haris/BigDataHadoopSpark/projet-spark/Project'
    file = os.path.join(data_dir,'lena_noisy.jpg')
    img_buf=readImg(file)
    print('SHAPE',img_buf.shape)
    #We have a 2D image. nx is the length. ny is the width. Since it's in color, 
    #each pixel is has 3 elements (RGB) from 0 to 255.
    nx=img_buf.shape[0]
    ny=img_buf.shape[1]
    print ("nx = {}, ny = {}".format(nx,ny))
    print('IMG\n', img_buf)
    
    ###########################################################################
    #
    # SPLIT IMAGES IN NB_PARTITIONS PARTS
    nb_partitions = 8
    print("NB PARTITIONS : ",nb_partitions)
    data=[]
    begin=0
    block_size=int(nx/nb_partitions)
    print ("blockSize = {}".format(block_size))
    for ip in range(nb_partitions):
        intermediaire = begin + block_size -1
        end=np.min([intermediaire,nx])
        data.append([ip,begin,end,img_buf])
        begin=end+1
    print ("Partitionnement shape : ",np.shape(data))
    print("Type de data :", type(data)) 
    
    ###########################################################################
    #
    # CREATE SPARKCONTEXT
    sc =SparkContext() 
    sc.setLogLevel("WARN")
    ###########################################################################
    #
    # PARALLEL MEDIAN FILTER COMPUTATION
    data_rdd = sc.parallelize(data, nb_partitions)
    #print(part_median_filter(data))
    print ("Type data_rdd:", type(data_rdd))
    start_spark = time.time()
    result_rdd = data_rdd.map(part_median_filter)
    end_spark = time.time()
    print("Type result_rdd:", type(result_rdd))
    result_data = result_rdd.collect()
    print("Type result_data:", type(result_data))
    #print("result data:", result_data[7])
    print("shape result_data", np.shape(result_data))
    #print("shape de la deuxieme colone de result_data", np.shape(result_data[0][1]))
    new_img_buf=np.zeros((nx,ny,3), dtype='uint8')
    
    partitions = 0
    for partition in range(len(result_data)):
        ip, buf = result_data[partition]
        for i in range(buf.shape[0]): #buf shape : (16,128,3)
            for j in range(ny):
                new_img_buf[partitions+i,j] = buf[i][j]
        partitions+= buf.shape[0]

    # Compare with python scipy :
    start_scipy = time.time()
    new_img_buf_scipy = median_filter(img_buf, (3,3,1))
    end_scipy = time.time()
    
    print('CREATE NEW PICTURE FILE SPARK') 
    filter_file = os.path.join(data_dir,'lena_filter.jpg')
    writeImg(filter_file,new_img_buf)

    
    print('CREATE NEW PICTURE SCIPY')
    filter_file_scipy = os.path.join(data_dir, 'lena_filter_scipy.jpg')
    writeImg(filter_file_scipy, new_img_buf_scipy)
    
    difference = np.zeros((nx,ny,3), dtype='uint8')
    zeros_count = 0
    mean_pixel=[0,0,0]
    variance_pixel =[0,0,0]
    for i in range(nx):
        for j in range(ny):
            for k in range(3):
                difference [i,j,k] = new_img_buf_scipy[i,j,k]-new_img_buf[i,j,k]
                if difference[i,j,k] == 0:
                    zeros_count+=1
                mean_pixel[k] += difference[i,j,k]
                variance_pixel[k] += difference[i,j,k]**2
    for h in range(3):
        mean_pixel[h] = mean_pixel[h]/(nx*ny*3)
        variance_pixel[h] = variance_pixel[h]/(nx*ny*3) - mean_pixel[h]**2
 
    print ("Difference Matrix between Spark and Scipy", difference)
    print ("number of zeros", zeros_count, "out of a total of ", nx*ny*3, "ie a zero ratio of : ", zeros_count/(nx*ny*3))
    print ("mean_pixel :", mean_pixel)
    print ("variance_pixel :", variance_pixel)
    print ("Time Spark : {}".format(end_spark-start_spark))
    print ("Time Scipy : {}".format(end_scipy-start_scipy))


if __name__ == '__main__':
    main()

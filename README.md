# Bloom-Filtering-Algorithm
Building Bloom Filtering Algorithm 

# The Requirements:
Assignment-2: Implementing a Bloom Filter using Spark RDDs

Objective
In this assignment, you will implement a distributed Bloom filter using Apache Spark RDDs. The goal is to gain hands-on experience with probabilistic data structures, Spark partitioning, and scalable hash-based storage mechanisms.

Background
A Bloom filter is a space-efficient probabilistic data structure used to test whether an element is a member of a set. False positives are possible, but false negatives are not. In this assignment, you will implement a scalable Bloom filter on top of Spark RDDs, enabling distributed insertion and lookup operations.

User Inputs
Before inserting any data, your program should accept the following input parameters:
Expected number of items (m) to be inserted into the Bloom filter.
Maximum allowed false positive probability (p).
Maximum number of records per partition (node) — this defines how RDD partitions should be distributed.
Using these parameters, your system should:
Compute the optimal Bloom filter size (n) in bits.
Compute the optimal number of hash functions (k).
Compute the required number of RDD partitions, based on the record limit per node.



Tasks
Task 1: Create a Large Set of Unique Items
Generate a large dataset (e.g., random integers, strings, or UUIDs) that will be used as the elements to insert into the Bloom filter.
Store this dataset as an RDD with a number of partitions determined automatically based on the “maximum records per partition” input.
Task 2: Implement the Distributed Bloom Filter
Represents the bit array as an RDD of bits or a bit vector broadcasted across partitions.
Insert all items from the dataset into the Bloom filter using RDD transformations.

Task 3: Lookup Facility
Implement a function that allows checking whether a given item may exist in the Bloom filter.
Demonstrate lookups for:
Items that are in the dataset (should mostly return True).
Items that are not in the dataset (should return False most of the time, but allow for a small false positive rate).
Task 4: Handling Capacity Overflow
Suppose the number of items to be inserted exceeds the initially expected number (m).
Propose and implement a scalable solution to handle additional items without reindexing the entire dataset.

Expected Output
Print or log:
The computed Bloom filter parameters (k, number of partitions).
The observed false positive rate from your test queries.
Demonstrate insertion and lookup with real timing and RDD partitioning statistics.

# Assignment 2: Bloom Filtering Report.

Lines 14-27,
The code starts by asking the user to enter the desired values of Expected number of items (m), Maximum allowed false positive probability (p), Maximum number of records per partition (node). 

<img width="1920" height="492" alt="image" src="https://github.com/user-attachments/assets/9741f60e-90a3-488a-b7f5-e39e511be940" />

Lines 31-54,
Calculation of optimal Bloom filter size (n), optimal number of hash functions (k), required number of RDD partitions. 
Afterwards, task 1, generation of long int values with respect of the predefined values. Then storing the generated data in a rdd in parallel.

Lines 56-81,
The main bloom filter code task2, where first we start by creating an empty rdd with all false values and respect to n and the number of partitions (rddPartNum). 
Then the creation of hash function, in this part I have used a predefined library for the hashing part (MurmurHash3). With consideration for the long int values since this is the type of the generated data.
Then using the hash function, hashing all the generated data, and storing it into the prepared rdd, and setting the values of the hashed numbers as True, which means that this number is stored in the bloom filter and it does exist.

Lines 82-93,
Task 3, testing the bloom filter, by looking up a certain given value.
the user provides a value, the program seach weather it exists in the bloom filter.
example of the logs:
where the provided value does not exist:

<img width="1926" height="344" alt="image" src="https://github.com/user-attachments/assets/18667e96-7e57-47c7-8711-07a24286bbfb" />


where the provided value exist (the head of the data):

<img width="1935" height="369" alt="image" src="https://github.com/user-attachments/assets/9fa2cbc8-ce56-42a8-ab68-dcf5453b5476" />


Lines 95-138,
Task 4: Handling Capacity Overflow, approach 1: The cost is low accuracy depends on the data size, where then we can lower the hash functions count (k) to maybe k-1 or k=1 depends on the m count,  then we store on top of the ones that already stored.
to test this, a testData parameter was created where first 10 are the top 10 values in the dataset, and the rest are values that does not exist.
and the m is 10 and this data is 20 (10 more than what the bloom filter expects)
val testData = data.take(10).toArray ++ Array(0,1,2,3,4,5,6,7,8,9)

<img width="1929" height="49" alt="image" src="https://github.com/user-attachments/assets/515267ff-bd41-4c6f-bcd9-af5608cccade" />

the logs:

<img width="1558" height="206" alt="image" src="https://github.com/user-attachments/assets/58829ce2-dcdc-4bcb-94d6-1a141c658a34" />


this approach is not scalable, and the cost is the accuracy, here comes the second approach.
In the code case the data size was only 20, that is why the true positive rate is high.

Lines 140-173,
Task 4, second approach have a scalable multi layer bloom filter, where we have the first main bloom filter and if the values exceeded the size, there would be additional one to cover and store the rest of the data (https://gsd.di.uminho.pt/members/cbm/ps/dbloom.pdf)

<img width="1923" height="212" alt="image" src="https://github.com/user-attachments/assets/0f6df88a-7e73-4be0-8652-e9d95ad3c156" />



## Resources 
https://en.wikipedia.org/wiki/Bloom_filter#Optimal_number_of_hash_functions

Probabilistic Data Structures slides by dr. Hamed Abdelhaq

https://en.wikipedia.org/wiki/Bloom_filter 

https://gsd.di.uminho.pt/members/cbm/ps/dbloom.pdf 

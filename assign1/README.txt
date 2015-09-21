AUTHORS: QI ZHAN, QIANQI GUAN, JINGYU ZHU.

Our implementation of storage manager is input into the storage_mgr.c

We added a test function testDoublePageContent() to the "test_assign1_1.c" file. In this function we tried to test all functions that we implemented, which are not included in the testSinglePageContent() function.

Our storage manager has the following functionalities: 

--------- manipulating page files 
-- create a file 
-- open a file
-- close a file
-- destroy a file

--------- reading blocks from disc 
-- get current page position in a file
-- Read the first page in a file
-- Read the last page in a file
-- Read previous page of current position
-- read current page in a file
-- read next page in a file from current position

--------- writing blocks to a page file 
-- Write a page to disk using the current position
-- Write a page to disk using absolute position.
-- Add one page to a file. The new last page will be filled with zero bytes.
-- increase the size to numberOfPages if the file has less than numberOfPages pages






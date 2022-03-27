# Summary
- In this file you'll find a section below on the users, brands and receipts files and how they were converted into postgres tables after extracting json data into individual columns.
- After that section, you'll find my solutions to questions 3 and 4 from the list. The data model used keeps in mind other requests coming into the system which need access to the brands table. The barcode column is used to create a relationship between the receipts and brands tables. Barcode was extracted from the column receipt items list.
- The diagram of this data model can be found in this repo as a `.png` file.
- Some data quality issues that i came across while working on these solutions are noted at the bottom.
- In the last section of this file you'll find a slack message I've drafted for stakeholders.


###1. Review unstructured JSON data and diagram a new structured relational data model

### USERS
- Unzipped the .gz files using `gunzip users.json.gz`
- Created a table with a single json field called `doc` which will accept all the json data as one column.
    ```
    create table users (doc json);
    ```
- Used psql to run the `\copy` command to load the users file into postgres
    ```
        \copy users from '/Users/me/Downloads/users.json' ;
    ``` 
- used arrow notation in postgres to access json fields in the `users.doc` column;. PostgreSQL provides two  operators `->` and `->>` to query JSON data. The operator `->` returns JSON object field as JSON. The operator `->>` returns JSON object field as text.                                           
    ```    
    select 
        doc -> '_id' ->> '$oid' as  id,
        (doc ->> 'active')::boolean as active,
        (doc -> 'createdDate' ->> '$date')::bigint as createdDate,
        (doc -> 'lastLogin' ->> '$date')::bigint as lastLogin,
        doc ->> 'role' as role,
        doc ->> 'signUpSource' as signUpSource,
        doc ->> 'state' as state
    from users;
    ```
- Since the `users` table only has one column at the moment, this raw data will be loaded into a new table `users1` which is split up into individual columns for each of the json keys in the users file
    ```
    create table users1(
        id text,
        active boolean,
        createdDate bigint,
        lastLogin bigint,
        role text,
        signUpSource text,
        state text
    );
    ```
- Inserted results of the json extracted query into the new table `users1`:
    ```
    insert into users1 (
    select 
        doc -> '_id' ->> '$oid' as  id,
        (doc ->> 'active')::boolean as active,
        (doc -> 'createdDate' ->> '$date')::bigint as createdDate,
        (doc -> 'lastLogin' ->> '$date')::bigint as lastLogin,
        doc ->> 'role' as role,
        doc ->> 'signUpSource' as signUpSource,
        doc ->> 'state' as state
    from users
    );
    ```

### BRANDS
- Unzipped the .gz files using `gunzip brands.json.gz`
- Created a table with a single json field called `doc` which will accept all the json data as one column.
    ```
    create table brands (doc json);
    ```
- Used psql to run the `\copy` command to load the brands file into postgres. This file contained some escaped quote characters which were rejected by the copy command. To solve this issue, the backslashes in the file were replaced with doubled backslashes, which would be converted back to single ones by the copy command. 
    ```
        \copy brands from '/Users/me/Downloads/brands.json' ;
    ``` 
- used arrow notation in postgres to access json fields in the `brands.doc` column;. PostgreSQL provides two  operators `->` and `->>` to query JSON data. The operator `->` returns JSON object field as JSON. The operator `->>` returns JSON object field as text.
    ```    
    select 
        doc -> '_id' ->> '$oid' as id,
        (doc ->> 'barcode')::bigint as barcode,
        doc -> 'brandCode' as brandCode,
        doc -> 'category' as category,
        doc -> 'categoryCode' as categoryCode,
        doc -> 'cpg' -> '$id' ->> '$oid' as cpg,
        doc -> 'name' as name,
        (doc ->> 'topBrand')::boolean as topBrand
    from brands;
    ```
- Since the `brands` table only has one column at the moment, this raw data will be loaded into a new table `brands1` which is split up into individual columns for each of the json keys in the brands file
    ```
    create table brands1 (
        id text,
        barcode bigint,
        brandCode text,
        category text,
        categoryCode text,
        cpg text,
        name text,
        topBrand boolean
    );

    insert into brands1 (
    select 
        doc -> '_id' ->> '$oid' as id,
        (doc ->> 'barcode')::bigint as barcode,
        doc -> 'brandCode' as brandCode,
        doc -> 'category' as category,
        doc -> 'categoryCode' as categoryCode,
        doc -> 'cpg' -> '$id' ->> '$oid' as cpg,
        doc -> 'name' as name,
        (doc ->> 'topBrand')::boolean as topBrand
    from brands
    );
    ```



### RECEIPTS

- Unzipped the .gz files using `gunzip receipts.json.gz`
- `create table receipts (doc json);`
- Used psql to run the `\copy` command to load the receipts file into postgres after correctly escaping the backslash character.
    - Can also use stream editor (sed) utility to replace all occurences of a backslash with a double backslash. I have tested this method locally and it works correctly.
    - `sed 's/\\/\\\\/g' receipts.json >> receipts_new.json`. [Source: StackOverflow](https://stackoverflow.com/a/67170003)

    ```
    \copy receipts from '/Users/me/Downloads/receipts_new.json' ;
    ``` 

- `receipts1` is split up into individual columns for each of the json keys in the receipts file
    ```
    create table receipts2 (
        _id text,
        bonusPointsEarned float,
        bonusPointsEarnedReason text,
        createDate bigint,
        dateScanned bigint,
        finishedDate bigint,
        modifyDate bigint,
        pointsAwardedDate bigint,
        pointsEarned float,
        purchaseDate bigint,
        purchasedItemCount int,
        rewardsReceiptItemList text,
        barcode text,
        rewardsReceiptStatus text,
        totalSpent float,
        userId text
    );

    insert into receipts2(
     select 
        doc -> '_id' ->> '$oid' as _id,
        (doc ->> 'bonusPointsEarned')::float as bonusPointsEarned,
        doc ->> 'bonusPointsEarnedReason' as bonusPointsEarnedReason,
        (doc -> 'createDate' ->> '$date')::bigint as createDate,
        (doc -> 'dateScanned' ->> '$date')::bigint as dateScanned,
        (doc -> 'finishedDate'->> '$date')::bigint as finishedDate,
        (doc -> 'modifyDate' ->> '$date')::bigint as modifyDate,
        (doc -> 'pointsAwardedDate' ->> '$date')::bigint as pointsAwardedDate,
        (doc ->> 'pointsEarned')::float as pointsEarned,
        (doc -> 'purchaseDate' ->> '$date')::bigint as purchaseDate,
        (doc ->> 'purchasedItemCount')::int as purchasedItemCount,
        doc ->> 'rewardsReceiptItemList' as rewardsReceiptItemList,
        reward_items.obj ->> 'barcode' as barcode,
        doc ->> 'rewardsReceiptStatus' as rewardsReceiptStatus,
        (doc ->> 'totalSpent')::float as totalSpent,
        doc ->> 'userId' as userId
    from receipts r
    cross join lateral json_array_elements(doc -> 'rewardsReceiptItemList') as reward_items(obj)
    where doc ->> 'rewardsReceiptItemList' is not null

    union 

     select 
        doc -> '_id' ->> '$oid' as _id,
        (doc ->> 'bonusPointsEarned')::float as bonusPointsEarned,
        doc ->> 'bonusPointsEarnedReason' as bonusPointsEarnedReason,
        (doc -> 'createDate' ->> '$date')::bigint as createDate,
        (doc -> 'dateScanned' ->> '$date')::bigint as dateScanned,
        (doc -> 'finishedDate'->> '$date')::bigint as finishedDate,
        (doc -> 'modifyDate' ->> '$date')::bigint as modifyDate,
        (doc -> 'pointsAwardedDate' ->> '$date')::bigint as pointsAwardedDate,
        (doc ->> 'pointsEarned')::float as pointsEarned,
        (doc -> 'purchaseDate' ->> '$date')::bigint as purchaseDate,
        (doc ->> 'purchasedItemCount')::int as purchasedItemCount,
        doc ->> 'rewardsReceiptItemList' as rewardsReceiptItemList,
        null as barcode,
        doc ->> 'rewardsReceiptStatus' as rewardsReceiptStatus,
        (doc ->> 'totalSpent')::float as totalSpent,
        doc ->> 'userId' as userId
    from receipts r
    where doc ->> 'rewardsReceiptItemList' is null);
    ```
- Here, a lateral cross join was used to access the barcode inside rewardsReceiptItemList so that a mapping between the receipts and brand tables can be made on the `barcode` field that exists in both tables. This barcode field was extracted from the nested json object `rewardsReceiptItemList` using a lateral cross join. 
Helpful links:
    - Official PostgreSQL Documentation for [JSON Functions](https://www.postgresql.org/docs/9.5/functions-json.html)
    - StackOverflow for [querying nested JSON](https://stackoverflow.com/a/63511022) 

```
    select
        reward_items.obj ->> 'barcode' as barcode,
    from receipts r
    cross join lateral json_array_elements(doc -> 'rewardsReceiptItemList') as reward_items(obj);
```



###2. Generate a query that answers a predetermined business question

####When considering average spend from receipts with 'rewardsReceiptStatus’ of ‘Accepted’ or ‘Rejected’, which is greater? 
```
 SELECT rewardsreceiptstatus, avg(totalspent) as avg_spent
 FROM receipts2
 WHERE rewardsreceiptstatus = 'REJECTED' or rewardsreceiptstatus = 'FINISHED'
 GROUP by rewardsreceiptstatus
 ORDER BY avg_spent desc
 LIMIT 1
;
```

#### When considering total number of items purchased from receipts with 'rewardsReceiptStatus’ of ‘Accepted’ or ‘Rejected’, which is greater?
```
SELECT rewardsreceiptstatus, sum(purchaseditemcount) as item_count
 FROM receipts2
 WHERE rewardsreceiptstatus = 'REJECTED' or rewardsreceiptstatus = 'FINISHED'
 GROUP by rewardsreceiptstatus
 ORDER BY item_count desc
 LIMIT 1;
```

###Data Quality Issues

1. there is a `fetch-staff` role in the users table. According to the data description in the html file provided, there should only have been one `consumer` role. Is this expected, and should rows with the staff role be removed from the final users table?
```
select distinct role, count(*) from users1 group by 1;
```

2. There is no `'ACCEPTED'` status in the receipts data, instead there is a `FINISHED` and `SUBMITTED`. Assuming that submitted has not yet reached the finished status, finished was used as a proxy for accepted.
```select distinct rewardsReceiptStatus, count(*) from receipts2 group by 1;```

3. There are 234 nulls, 35 empty strings, and various different values for the brand code field in `brands` that are either IDs, or names of brands. There are also a few test brandcodes in this column that might be left over from a test on the data.

```select distinct brandCode, count(*) from brands1 group by 1 order by 2 desc;```


5. The timestamps provided are in miliseconds, and need to be divided by 1000 to get the correct conversion to a human readable date. In the query below, the third column which divides the timestamp by 1000 results in the correct date values.
```select createddate, to_timestamp(createddate), to_timestamp(createddate/1000) from users1;```

6. There are 440 records in receipts that have no items on the receipt, and all of them correspond to the status of `SUBMITTED`. These null item lists cause issues when flattening the json items array to extract the barcode. What does the submitted status correspond to, and should these records be in this receipts table if they have no items on them?
```
select
    _id,
    bonuspointsearned,
    bonuspointsearnedreason,
    to_timestamp(createdate/1000) as createdate,
    to_timestamp(datescanned/1000) as datescanned,
    to_timestamp(finisheddate/1000) as finisheddate,
    to_timestamp(modifydate/1000) as modifydate,
    to_timestamp(pointsawardeddate/1000) as pointsawardeddate,
    pointsearned,
    to_timestamp(purchasedate/1000) as purchasedate,
    purchaseditemcount,
    rewardsreceiptitemlist,
    barcode,
    rewardsreceiptstatus,
    totalspent,
    userid
from receipts2 where rewardsReceiptItemList is null;
```

7. These records indicate that no items were purchased but the user may have earned points on this receipt. Want to clarify if this is a valid case, or if points were assigned by error?
```
select
    distinct bonuspointsearnedreason, bonuspointsearned, pointsEarned, purchasedItemCount
from receipts2
where purchasedItemCount < 1;
```

###Message to Stakeholder

Hello! I'm working on the brands users and receipts data files, and have a few clarifying questions before I can proceed. I'll list them out one by one below:

1. There is a `fetch-staff` role in the users file. According to the data description provided, there should only have been one `consumer` role. Is this expected, and should rows with the staff role be removed from the final users table?

2. There is no `ACCEPTED` status in the `receipts` data file, instead there is a `FINISHED` and `SUBMITTED`.  Is `FINISHED` equivalent to `ACCEPTED`? What purpose do the records with `SUBMITTED` status serve?
There are 440 records that have no items on the receipt, and all of them correspond to the status of `SUBMITTED` - should these records be in this receipts table if they have no items on them?

3. There are some nulls, empty strings, numeric IDs, and brand names available in the brand code field in the `brands` file. This field appears to lack some consistency. Can I have access to the product file to confirm whether this is a brand name or a brand ID field, and to ensure that the values in both files match accurately?

4. The `brands` file has a category for "beauty" and for "beauty and personal care" that might have potential for being grouped - do they need to remain separate/are these values corresponding to values in the partners file mentioned before as well?

5. Some records on the receipts data indicate that no items were purchased but the user may have earned points on this receipt. Want to clarify if this is a valid case, or if points were assigned by error? Also, the bonus points earned reason column makes references to receipt numbers 1 through 6 - are these test values, or do they correspond to something specific in production data?
```
    bonuspointsearnedreason, bonuspointsearned, pointsearned, purchaseditemcount
    "Receipt number 2 completed, bonus point schedule DEFAULT (5cefdcacf3693e0b50e83a36)",500,500,0
    "Receipt number 3 completed, bonus point schedule DEFAULT (5cefdcacf3693e0b50e83a36)",250,250,0
```

6. I anticipate that the receipts table will be very large in production. The data model that I'm following expands the receipts table by flattening the item list array into individual records to gain access to the barcode field. This barcode field is used as a key to map between brands and receipts. Since a receipt can have > 1 items, this table could grow very large and unruly (with lots of duplication) if we continue flattening each item list into individual rows for barcodes. Instead, it may make more sense to create a new table of just the receipt ID and the flattened barcode, that will act as the normalized link between receipts and brands tables.


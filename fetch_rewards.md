## USERS
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

## BRANDS
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



## RECEIPTS

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






### solution 3
```

```
 SELECT rewardsreceiptstatus, avg(totalspent) as avg_spent
 FROM receipts2
 WHERE rewardsreceiptstatus = 'REJECTED' or rewardsreceiptstatus = 'FINISHED'
 GROUP by rewardsreceiptstatus
 ORDER BY avg_spent desc
 LIMIT 1
;


### solution 4
SELECT rewardsreceiptstatus, sum(purchaseditemcount) as item_count
 FROM receipts2
 WHERE rewardsreceiptstatus = 'REJECTED' or rewardsreceiptstatus = 'FINISHED'
 GROUP by rewardsreceiptstatus
 ORDER BY item_count desc
 LIMIT 1;




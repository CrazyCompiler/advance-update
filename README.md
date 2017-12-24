# Advance Update

Control your elasticsearch document updates with extra speed.

## Description

 - So, Elasticsearch provides update API's like _udate and _bulk which helps us in updating elasticsearch document.
 But elasticsearch updates the document by merging the given **[NEW]** document with the **[OLD]** document, which leads to
  a case where
    - The older documents fields which are not present in the new given document will be retained, so if you want
      delete a field you have to explicitly send a update query with a script. And everyone knows that elasticsearch
      scripts are slower (eg: to delete a field from 13 million documents it will take 50 min in 5 node cluster)

     eg : _Old Document_

            {
                "a": 23,
                "b": 40
            }
       _New Document_ : /_update api

            {
                "a": 19,
                "c": 45
            }

       Now If I send this to elasticsearch the result output document will be :

            {
                "a": 19,
                "b": 40,
                "c": 45
            }

       So the field "b"is retained, but my usecase is to remove the "b", as I not sending it in the request.
     - The Advance Plugin can do this for you.

     eg : _Old Document_

            {
                "a": 23,
                "b": 40
            }
       _New Document_ : /_advanceupdate api

            {
                "a": 19,
                "c": 45
            }

       Now If I send this to elasticsearch the result output document will be :

            {
                "a": 19,
                "c": 45
            }

     - Elasticsearch Updates first update documents in the primary shard and then
     it updates the replica shards, now that increased the total time required for
     update of document.
     - You can reduce the total time by setting the replica to -1, but if you have a lot
     of documents which takes 1 hour to get updated and in meanwhile your nodes goes down
     you dont have a protection of the replicas to recover i.e, you loose your data.
     - Advance-Update gives you the functionality where your old data is safe and
     speed for updating documents is much higher than the normal updates.

### API Support

 - _advanceupdate

    usage :

        /_update

        {
            "a": 19,
            "c": 45
        }


 - _advancebulk

        /_advancebulk

         { "update" : {"_id" : "2", "_type" : "type1", "_index" : "test"} }
         { "doc" : {"b": 12,"f": 14,"m":15}, "doc_as_upsert" : true}
         { "update" : {"_id" : "2", "_type" : "type1", "_index" : "test"} }
         { "doc" : {"s": 15,"l": 14,"k":12}, "doc_as_upsert" : true}





### Prerequisites

Elasticsearch 5.6.0

### Installing

Download and install elasticsearch 5.6.0 from
* [here](https://www.elastic.co/blog/elasticsearch-5-6-0-released)

Download the leatest Advance Update plugin from here.

Install the plugin :


```
    sudo bin/elasticsearch-plugin install file:///Downloads/advance-update.zip
```

## Versioning

Works with only elasticsearch 5.6.0

## Authors

* **Viraj Parab** - *Initial work* - [CrazyCompiler](https://github.com/CrazyCompiler)

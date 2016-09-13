var AWS = require('aws-sdk')
var dd = new AWS.DynamoDB();
var tableName = 'test'

exports.handler = (event, context) => {
    
    function parseBucketName( record ){
        return record.s3.bucket.name 
    }

    function parseObjectKey( record ){
        // Object key may have spaces or unicode non-ASCII characters.
        return decodeURIComponent(record.s3.object.key.replace(/\+/g, " "))
    }

    function parseRegion( record ){
        return record.awsRegion
    }
    
    try{
        putItem = function(bucket, objectKey, region){
            console.log(" putItem called " + objectKey)
            
            var item = {
                    "Filename":{
                        "S":objectKey
                        
                    },
                    "Bucket":{
                        "S":bucket
                        
                    },
                    "Region":{
                        "S":region
                    }
                }
            
            var res = dd.putItem({
                'TableName':tableName,
                'Item': item}, function(err,data){
                    if(err){
                        context.fail('putItem response error::'+err)
                    }else{
                        context.succeed('putItem OK')
                    }
                })
        }
        
        for (var index = 0, limit = event.Records.length; index < limit; index++) {
            
            var record = event.Records[index]
            var bucket = parseBucketName(event.Records[index] )
            var objectKey = parseObjectKey(event.Records[index] )
            var region = parseRegion(event.Records[index])
            
            putItem(bucket, objectKey, region)
         }
    }
    catch(error){
        context.fail("PutItem error::"+error)
    }    
}

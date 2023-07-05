const { Client: Client7 } = require('es7')
const { ES } = require('../conf.json');

/**
 *  Utilizat pentru import in Elasticsearch
 * @param indexul in care se doreste a se importa datele _index 
 * @param datele de importat _data 
 */

const client = new Client7({ node: ES.IP });

const closeElasticClient = () => {
    client.close();
  };
module.exports.closeElasticClient = closeElasticClient;

const insertElastic = async (data, index_dest) => {
    try {
        const client = new Client7({ node: ES.IP })

        return await client.index({
            index: index_dest,
            body: data
        });
    } catch (error) {
        console.log(error)
        return {
            err: true,
            errMsg: error,
            data: _data
        };
    }

}
module.exports.insertElastic = insertElastic;

const searchElastic = async (search, index_dest) => {
    try {
        const client = new Client7({ node: ES.IP })
        let { body } = await client.search({
            index: index_dest,
            body: search
        })
        console.log(body);
        return body;
    } catch (error) {
        console.log(error)
        return null;
    }

}
module.exports.searchElastic = searchElastic;


const insertBulkElastic = async ( insertArray = [], index_dest) => {
    try {
       
        let bulkData = [];
        console.log(insertArray);
        console.log("---");
        for (let i = 0; i < insertArray.length; i++) {

                bulkData.push({ index: { _index: index_dest } });
                bulkData.push(insertArray[i]);
            }
        
        if (bulkData.length === 0)
            return {
                body: []
            }
        let response = await client.bulk({
            body: bulkData
        });
        
        return response;
    } catch (error) {
        console.log(error)
        return new Error('Eroare inserare bulk!');
    }

}
module.exports.insertBulkElastic = insertBulkElastic;

const deleteElastic = async (searchQuery, index_dest) => {
    try {
      const client = new Client7({ node: ES.IP });
  
      const { body: searchResponse } = await client.search({
        index: index_dest,
        body: {
          query: searchQuery
        }
      });
  
      const deleteIds = searchResponse.hits.hits.map(hit => hit._id);
  
      const { body: deleteResponse } = await client.deleteByQuery({
        index: index_dest,
        body: {
          query: searchQuery
        }
      });
  
      await client.close();
  
      console.log(`Deleted ${deleteIds.length} records from index ${index_dest}.`);
  
      return deleteResponse;
    } catch (error) {
      console.error('Error deleting records:', error);
      throw new Error('Error deleting records.');
    }
  };
  
  module.exports.deleteElastic = deleteElastic;


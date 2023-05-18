const { Client: Client7 } = require('es7')
const { ES } = require('../conf.json');

/**
 *  Utilizat pentru import in Elasticsearch
 * @param indexul in care se doreste a se importa datele _index 
 * @param datele de importat _data 
 */

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
        return body;
    } catch (error) {
        console.log(error)
        return null;
    }

}
module.exports.searchElastic = searchElastic;


const insertBulkElastic = async (data = [], index_dest) => {
    try {
        const client = new Client7({ node: ES.IP })
        let bulkData = [];
        for (let i = 0; i < data.length; i++) {
            for (let j = 0; j < data[i].data.length; j++) {
                bulkData.push(data[i].data[j]);
            }
        }
        
        if (bulkData.length === 0)
            return {
                body: []
            }
        let response = await client.bulk({
            index: index_dest,
            body: bulkData
        });

        await client.close();
        return response;
    } catch (error) {
        console.log(error)
        throw new Error('Eroare inserare bulk!')
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


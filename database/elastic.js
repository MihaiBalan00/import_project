const { Client: Client7 } = require('es7')
const { ES } = require('../conf.json');

const insertBulkElastic = async ( insertArray = [], index_dest) => {
    const client = new Client7({ node: ES.IP });
    try {
        
        let bulkData = [];
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
    finally{
        await client.close();
    }

}
module.exports.insertBulkElastic = insertBulkElastic;




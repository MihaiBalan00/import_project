const express = require('express');
const fileImporter = require('../controller/import-files');

const router = express.Router();

router.post('/fileImport', fileImporter.mainImporter);


module.exports = router;
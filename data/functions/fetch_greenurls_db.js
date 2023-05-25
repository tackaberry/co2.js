const fs = require("fs");
const stream = require('stream');
const sqlite = require('sqlite3');
const { promisify } = require('util');
const zlib = require('zlib');
const https = require("https")

async function getBody(url) {
    return new Promise(function (resolve, reject) {
      // Do async job
      const req = https.get(url, function (res) {
        if (res.statusCode < 200 || res.statusCode >= 300) {
          return reject(new Error(`Status Code: ${res.statusCode}`));
        }
        const data = [];
  
        res.on("data", (chunk) => {
          data.push(chunk);
        });
  
        res.on("end", () => resolve(Buffer.concat(data).toString()));
      });
      req.end();
    });
  }

async function getStream(url) {
    return new Promise(function (resolve, reject) {
        const req = https.get(url, function (res) {
            resolve(res);
          });
        req.end();
    })
}
  
class DBStream extends stream.Readable {

    constructor( opts ) {
        super( { objectMode: true } );
        this.sql = opts.sql;
        this.db = new sqlite.Database( opts.db );
        this.stmt = this.db.prepare( this.sql );
        this.on( 'end', () => this.stmt.finalize( () => this.db.close() ));
    }
    _read() {
        let strm = this;
        this.stmt.get( function(err,result) {
            err ?
                strm.emit('error', err ) :
                strm.push( result || null);
       })
    }
} 

let dateSearch = null
if(process.argv[2]){
    const arg = process.argv[2]
    if(arg.match(/\d{4}-\d{1,2}-\d{1,2}/)){
        dateSearch = arg
        console.log(`finding closest dataset to ${dateSearch}`);
    }
}

(async function run() {
    try {
        const body = await getBody("https://api.thegreenwebfoundation.org/admin/green-urls")
        const links = body.matchAll(/https.*?green[-_]urls[-_]([0-9-]+)(\.db)?\.(gz)/gi)
        let latest = null
        let recent = dateSearch ? dateSearch : null
        let earliest = dateSearch ? true: false

        for (link of links) {
            let [ url, date, db, ext] = link
            const d1 = new Date(recent)
            const d2 = new Date(date)
            if( d2.getTime() > d1.getTime() ){
                recent = date
                latest = url
                if(earliest)  break
            }
        }
        console.log(`found latest source: ${latest}`);

        const httpStream = await getStream(latest)

        const pipelineAsync = promisify(stream.pipeline);

        await pipelineAsync(
            httpStream,
            new stream.Transform({ transform: (chunk, encoding, next) => { next(null, chunk);  }, }),
            fs.createWriteStream('domains.db.gz'),
        )
        console.log("file downloaded.");

        await pipelineAsync(
            fs.createReadStream('domains.db.gz'),
            zlib.createUnzip(),
            fs.createWriteStream('domains.db'),
        );
        console.log("db ready.");

        let c=0
        const readable = new DBStream( { sql:'select * from greendomain limit 1000', db: "domains.db" } )
        const transform = new stream.Transform( { 
            objectMode: true, 
            transform:( data, enc, cb ) => cb( null, `${(++c)!==1?",\n  ":"  "}${JSON.stringify( data )}` ), 
            flush:( cb ) => cb( null, "\n]}\n" ) } 
        )
        const writable = fs.createWriteStream("domains.json");

        writable.write(`{"data":[\n`)
        await pipelineAsync(
            readable,
            transform,
            writable,
        );

        console.log("write json file completed.");
    }
 
    // Shows error
    catch (err) {
        console.error('pipeline failed with error:', err);
    }
})();
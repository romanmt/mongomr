var mongolian  = require('mongolian')
  , mongo      = new mongolian
  , underscore = global._ = require('underscore')
  , inspect    = global.inspect = require('eyes').inspector()
  , db         = global.db = mongo.db("mapreduce-development")
  , async      = require('async')
  , Faker      = require('Faker');

var products = db.collection("products");
var randomized = db.collection("randomized");
var plist = db.collection("plist");

var mapInjectRandom = function() {
  emit(this._id, 
      { _id    : this._id
      , random : Math.random()
      });
};

var mapPlist = function() {
  emit(this._id, 
      { _id    : this.value._id
      , random : this.value.random 
      });
}

var reduce = function(k, vals) {
  return vals;
};

var dropProducts = function (callback) {
    console.log("drop products");
    products.drop( function (){
        callback(null);
    });
};

var createProducts =  function (callback) {
    console.log("create products");
    for (i = 0; i < 20; i++) {
        products.save( { product: Faker.Company.bs()
                       , company: Faker.Company.companyName()
        });
    }
    callback(null);
};

var injectRandomAttribute = function (callback) {  
    console.log("inject random");
    products.mapReduce(mapInjectRandom, reduce, {out: 'randomized'}, callback);
};

var selectProductIds = function (res, callback) {
    console.log("select prod list");
    var sample = Math.random();
    randomized.mapReduce(mapPlist, reduce,
          { out   : 'plist'
          , query : {'value.random' : {$gte : sample}}
          , limit : 6},
          function (err, res) { callback(err, res, sample) });
};

var selectBackwards = function (res, sample, callback) {
    var count = res.counts.output;
    if (count < 6) {
        console.log("not enough results ("+count+" of 6), getting more...");
        randomized.mapReduce(mapPlist, reduce,
            { out   : {merge : 'plist'}
            , query : {'value.random' : {$lt : sample}}
            , limit : 6 - count},
            callback);
    } else {
      callback(null, res);
    } 
};

var selectProducts = function (res, callback) {
    console.log("select products");
    plist.find().toArray(function (err, values) {
        var ids = _.map(values, function(x) {return x.value._id} );
        inspect(ids);
        products.find({_id : {$in : ids}}).toArray(callback);
    });
};

var printResults = function (err, prods) {
    if (err) throw err;
    console.log("print products");
    inspect(_.map(prods, function (x) {return x.product}));
};

async.waterfall(
    [ injectRandomAttribute
    , selectProductIds
    , selectBackwards
    , selectProducts
    ]
    , printResults);


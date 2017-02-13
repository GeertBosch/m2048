let moveRight = (x) => parseInt("0" + x.toString(16).replace(/0/g, ''), 16);
let moveLeft = (x) => parseInt((x.toString(16).replace(/0/g, '') + "0000").substr(0, 4), 16);
let rowBlanks = (x) => 15 - parseInt(x.toString(16).replace(/[^0]/g, "1"), 2);
let cellText = (x) => (x ? "    " + (1 << x) + (x < 14 ? " " : "") : "     ").substr(-5);
let rowText = (x) => cellText(x >> 12) + "|" + cellText(x >> 8 & 0xf) + "|" +
    cellText(x >> 4 & 0xf) + "|" + cellText(x & 0xf);
let rowDoc = (x) => {
    return {
        _id: x, blanks: rowBlanks(x), ascii: rowText(x), right: mergeRight(x), left: mergeLeft(x)
    }
};

function mergeRight(x) {
    x = moveRight(x);
    let score = 0;
    for (let j = 0; j < 12; j += 4) {
        let l = x >> (j + 4) & 0xf;
        let r = x >> j & 0xf;
        if (r && l == r) {
            x = x - (l << (j + 4)) + (1 << j);
            score += 1 << (r + 1);
        }
    }
    return {
        score, row: moveRight(x)
    }
}

function mergeLeft(x) {
    x = moveLeft(x);
    let score = 0;
    for (let j = 12; j > 0; j -= 4) {
        let l = x >> j & 0xf;
        let r = x >> (j - 4) & 0xf;
        if (l && l == r) {
            x = x - (r << j - 4) + (1 << j);
            score += 1 << (r + 1);
        }
    }
    return {
        score, row: moveLeft(x)
    }
}

function createRows(coll) {
    db[coll].bulkWrite(
        new Array(65536).fill(0).map((x, y) => {return {insertOne: {document: rowDoc(y)}}}));
}

function saveFunctions(funs) {
    funs = funs || db.system.js.find({value: {$ne: null}}, {_id: 1}).toArray().map((x) => x._id);
    funs = funs instanceof Array ? funs : [funs];
    funs.forEach((x) => db.system.js.save({_id: x, value: this[x]}));
}

function printFunctions(funs) {
    funs = funs || db.system.js.find({value: {$ne: null}}, {_id: 1}).toArray().map((x) => x._id);
    funs = funs instanceof Array ? funs : [funs];
    funs.forEach((x) => this[x].prototype ? print(tojson(this[x]) + "\n")
                                          : print("let " + x + " = " + tojson(this[x]) + "\n"));
}

function getPipes() {
    let pipes = {};
    pipes.ascii = [
        {"$unwind": {"path": "$grid", "includeArrayIndex": "i"}},
        {"$lookup": {"from": "rows", "localField": "grid", "foreignField": "_id", "as": "rows"}},
        {"$group": {"_id": "$_id", "grid": {"$push": "$rows.ascii"}, score: {$first: "$score"}}}
    ];

    pipes.expand = [
        {"$unwind": "$grid"},
        {
          "$addFields": {
              "grid": [
                  {"$floor": {"$divide": ["$grid", 4096]}},
                  {"$mod": [{"$floor": {"$divide": ["$grid", 256]}}, 16]},
                  {"$mod": [{"$floor": {"$divide": ["$grid", 16]}}, 16]},
                  {"$mod": ["$grid", 16]}
              ]
          }
        },
        {"$group": {"_id": "$_id", "grid": {"$push": "$grid"}, "score": {"$first": "$score"}}}
    ];

    pipes.compress = [
        {"$unwind": "$grid"},
        {
          "$addFields": {
              "grid": {
                  "$add": [
                      {"$multiply": [{"$arrayElemAt": ["$grid", 0]}, 4096]},
                      {"$multiply": [{"$arrayElemAt": ["$grid", 1]}, 256]},
                      {"$multiply": [{"$arrayElemAt": ["$grid", 2]}, 16]},
                      {"$arrayElemAt": ["$grid", 3]}
                  ]
              }
          }
        },
        {"$group": {"_id": "$_id", "grid": {"$push": "$grid"}, "score": {"$first": "$score"}}}
    ];

    let transposeStage = {
        "$project": {
            "grid": {
                "$zip": {
                    "inputs": [
                        {"$arrayElemAt": ["$grid", 0]},
                        {"$arrayElemAt": ["$grid", 1]},
                        {"$arrayElemAt": ["$grid", 2]},
                        {"$arrayElemAt": ["$grid", 3]}
                    ]
                }
            },
            "score": 1
        }
    };

    pipes.transpose = pipes.expand.concat(transposeStage, pipes.compress);

    pipes.merge = {};

    pipes.merge.right = [
        {"$unwind": "$grid"},
        {"$lookup": {"from": "rows", "localField": "grid", "foreignField": "_id", "as": "row"}},
        {
          "$group": {
              "_id": "$_id",
              "grid": {"$push": {"$arrayElemAt": ["$row.right.row", 0]}},
              "oldgrid": {"$push": "$grid"},
              "score": {"$sum": {"$arrayElemAt": ["$row.right.score", 0]}},
              "oldScore": {"$first": "$score"}
          }
        },
        {
          "$project": {
              "grid": 1,
              changed: {$ne: ["$grid", "$oldgrid"]}, "score": {"$add": ["$score", "$oldScore"]}
          }
        },
        {"$match": {changed: true}},
        {"$project": {change: 0}},
    ];

    pipes.merge.left = [
        {"$unwind": "$grid"},
        {"$lookup": {"from": "rows", "localField": "grid", "foreignField": "_id", "as": "row"}},
        {
          "$group": {
              "_id": "$_id",
              "grid": {"$push": {"$arrayElemAt": ["$row.left.row", 0]}},
              "oldgrid": {"$push": "$grid"},
              "score": {"$sum": {"$arrayElemAt": ["$row.left.score", 0]}},
              "oldScore": {"$first": "$score"}
          }
        },
        {
          "$project": {
              "grid": 1,
              changed: {$ne: ["$grid", "$oldgrid"]}, "score": {"$add": ["$score", "$oldScore"]}
          }
        },
        {"$match": {changed: true}},
        {"$project": {change: 0}},
    ];

    pipes.merge.down = pipes.transpose.concat(pipes.merge.left, pipes.transpose);
    pipes.merge.up = pipes.transpose.concat(pipes.merge.left, pipes.transpose);

    pipes.nextMove = [
        {$facet: pipes.merge},
        {
          $project: {
              moves: [
                  {move: "l", grid: "$left"},
                  {move: "r", grid: "$right"},
                  {move: "d", grid: "$down"},
                  {move: "u", grid: "$up"}
              ]
          }
        },
        {$unwind: "$moves"},
        {$unwind: "$moves.grid"},
        {
          $project: {
              _id: {$concat: ["$moves.grid._id", " ", "$moves.move"]},
              grid: "$moves.grid.grid",
              score: "$moves.grid.score"
          }
        }
    ];
    return pipes;
}

function m2048() {
    let pipes = getPipes();
    db.nexttest.drop();
    db.createView("nexttest", "game", pipes.nextMove);
}

function status(funs) {
    funs = funs || db.system.js.find({value: {$ne: null}}, {_id: 1}).toArray().map((x) => x._id);
    funs = funs instanceof Array ? funs : [funs];

    let conflicts = [];
    let stale = [];
    let modified = [];
    let added = [];
    let deleted = [];
    let uptodate = [];
    let last = {};

    let checkedOut = this["checkedOut"] || {};
    let checkedIn = {};

    db.system.js.find().forEach((x) => {checkedIn[x._id] = x.value.code});

    for (name of funs) {
        let base = checkedOut[name] || null;
        let theirs = checkedIn[name] || null;
        let mine = this[name] instanceof Function ? tojson(this[name]) : null;

        if (base != theirs && base != mine && theirs != mine)
            conflicts.push(name);
        else if (base != theirs && base == mine)
            stale.push(name);
        else if (base == null && theirs == mine)
            stale.push(name);
        else if (base == null && theirs == null && mine != null)
            added.push(name);
        else if (base != null && theirs == base && mine == null)
            deleted.push(name);
        else if (base == theirs && base != mine)
            modified.push(name);
        else if (base == theirs && base == mine && base != null)
            uptodate.push(name);
        else
            return {ok: 0, base, theirs, mine};
    }
    return {ok: 1 * (conflicts.length == 0), stale, modified, added, deleted, conflicts};
}

function checkout(funs) {
    let s = status(funs);
    if (s.stale.length + s.deleted.length == 0)
        return s;

    let checkedOut = this["checkedOut"] || {};
    if (!this["checkedOut"])
        this["checkedOut"] = checkedOut;

    let updated = [];

    db.system.js.find({_id: {$in: s.stale.concat(s.deleted)}}).forEach((x) => {
        updated.push(x._id);
        checkedOut[x._id] = x.value.code;
        if (x.value.constructor === Code) {
            this[x._id] = eval("(" + x.value.code + ")");
        } else {
            this[x._id] = x.value;
        }
    });
    s = status(funs);
    if (s.stale.length + s.deleted.length != 0) {
        s["ok"] = 0;
        return s;
    }

    return {
        ok: 1,
        updated,
        modified: s.modified,
        added: s.added,
        deleted: s.deleted,
        conflicts: s.conflicts
    };
}

function commit(funs, message) {
    let s = status(funs);
    s.ok = 0;
    if (s.stale.length + s.conflicts.length == 0)
        return s;
    log = [];
}

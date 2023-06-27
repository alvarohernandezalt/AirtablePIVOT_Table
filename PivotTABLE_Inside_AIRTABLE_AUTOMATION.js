let Airtable = require('airtable');
let baseKey = input.config('baseKey');
let oportunidadesTableName = input.config('oportunidadesTableName');
let valoresPivotTableName = input.config('valoresPivotTableName');

let apiKey = input.config('apiKey');
let base = new Airtable({ apiKey: apiKey }).base(baseKey);
let valoresPivotBase = new Airtable({ apiKey: apiKey }).base(baseKey);

// Get all data from 'Oportunidades' table
base(oportunidadesTableName).select().all(function(err, records) {
  if (err) {
    console.error(err);
    return;
  }
  
  // Convert records to a DataFrame-like structure
  let data = records.map(record => record.fields);
  let df = data.reduce((acc, row) => {
    Object.keys(row).forEach(key => {
      if (!acc.hasOwnProperty(key)) {
        acc[key] = [];
      }
      acc[key].push(row[key]);
    });
    return acc;
  }, {});

  // Create a DataFrame-like object
  let columns = Object.keys(df);
  let values = columns.map(col => df[col]);
  let dfObj = { columns, values };

  // Pivot table with Estado as rows and Prioridad as columns
  let pt = pivotTable(dfObj, 'Estado', 'Prioridad', 'Presupuesto_VAL', 'sum');

  // Insert pivot table values into 'Valores_PIVOT' table
  let recordsToInsert = [];
  for (let estado in pt) {
    for (let prioridad in pt[estado]) {
      let presupuestoVal = pt[estado][prioridad];
      let record = { 'Estado': estado, 'Prioridad': prioridad, 'Presupuesto_VAL': presupuestoVal };
      recordsToInsert.push(record);
    }
  }

  valoresPivotBase(valoresPivotTableName).create(recordsToInsert, function(err, records) {
    if (err) {
      console.error(err);
      output.set('errorMessage', err.message);
      return;
    }
    output.set('recordsInserted', records.length);
    console.log('Records inserted:', records.length);
  });
});

function pivotTable(df, index, columns, values, aggfunc) {
  let indexArr = df.values[df.columns.indexOf(index)];
  let columnsArr = df.values[df.columns.indexOf(columns)];
  let valuesArr = df.values[df.columns.indexOf(values)];

  let pivot = {};
  for (let i = 0; i < indexArr.length; i++) {
    let row = indexArr[i];
    let col = columnsArr[i];
    let val = valuesArr[i];

    if (!pivot.hasOwnProperty(row)) {
      pivot[row] = {};
    }

    if (!pivot[row].hasOwnProperty(col)) {
      pivot[row][col] = [];
    }
    pivot[row][col].push(val);
  }

  let result = {};
  for (let row in pivot) {
    result[row] = {};
    for (let col in pivot[row]) {
      let values = pivot[row][col];
      let aggValue = aggregate(values, aggfunc);
      result[row][col] = aggValue;
    }
  }

  return result;
}

function aggregate(values, aggfunc) {
  if (aggfunc === 'sum') {
    return values.reduce((acc, val) => acc + val, 0);
  } else if (aggfunc === 'mean') {
    let sum = values.reduce((acc, val) => acc + val, 0);
    return sum / values

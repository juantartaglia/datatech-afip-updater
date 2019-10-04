const sql= require('mssql');
const config = require('./config.local.json');
const fs = require('fs');
const separador = '-';
const pdf_table_extractor = require("pdf-table-extractor");
let cuitsDatatech = [];
let conn =  sql.connect(config.sqlServer);

let getData = new Promise((resolve, reject) => {
    let sqlQuery = `SELECT DISTINCT CUI FROM CLINST`;
    console.log("Ejecutando consulta");
    conn.then(pool => {
        pool.request().query(sqlQuery).then(res => {
            resolve(res.recordset)
        }).catch(err => {
            reject(err);
        })
    });
});


let readPdf = (cuitsDatatech)=> {
    return new Promise((resolve, reject) => {
        console.log("Parseando Pdf ..");
        pdf_table_extractor("LISTADO AFIP 23-09-2019.pdf",(result)=> {
            let cuits = [];
            let data = result.pageTables;
            let tables = data.map((item)=>{ return item.tables});
            let valores =tables.map((rows)=>{
                return rows.map((v)=>{
                    return v[0];
                })
            });
            valores.forEach(element => 
                element.forEach(item => {
                    if (item.length == 11)
                    cuits.push(item)
                }) 
            );
            let final = cuitsDatatech.map(elemento => {
                let cuitData = elemento.CUI.trim();
                cuitData = separador == '-' ? cuitData.replace(/-/g,"") : cuitData;
                return cuits.find(cuit => cuit == cuitData);
            })
            resolve(final.filter(c =>  c != undefined ));
        },(err)=>{
            reject(err);
        });
    });
}
    
function updateSql(sql) {
    return new Promise((resolve, reject)=>{
            conn.then(pool => {
                pool.request().query(sql).then((res)=> resolve(res.rowsAffected) ).catch((err)=> reject(err))
            })    
        
        })
}


getData.then(res => {
    readPdf(res).then(result =>{
        let cuitsDataUpdate = result.map(element => element.substr(0,2)+separador+element.substr(2,8)+separador+element[10]); 
        cuitsDataUpdate.forEach(item => {
            let sql = `UPDATE CLINST SET GEM='S' WHERE CUI='${item}'`;
            updateSql(sql).then(res => console.log("Registros actualizados ("+res+")")).catch(err => console.log(err))
            console.log(sql);
        });
        console.log("Finalizado!")
    }).catch((err)=> console.log("Ocurrio un error al leer el archivo"+err));
}).catch(err => console.log("Ocurrio un error en la query"+err));


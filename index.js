const fs = require('fs');
const csv = require('csv-parser');

const carpeta = './records-csv/';
const rutaArchivoDDL = './ddl/ddl-gestor.sql';
const carpetaScripts = './scripts/';
const tablas = [];
const TIMESTAMP = 'TIMESTAMP';
const NUMBER = 'NUMBER';
const VARCHAR2 = 'VARCHAR2';

function generarInserts(archivo, nombreTabla) {
    const inserts = [];

    fs.createReadStream(archivo)
        .pipe(csv())
        .on('data', (row) => {
            const values = Object.values(row).map(value => `'${value}'`).join(', ');
            const tablaEncontrada = tablas.find(tabla => tabla['nombreTabla'] == nombreTabla);
            const campos = Object.keys(row);
            const valores = Object.values(row);
            const campoTipoValor = campos.map((campo, indice) => {
                const definicionCampo = tablaEncontrada['lineasCreate'].find(linea => linea.includes(campo));
                const tipo = definicionCampo.includes(TIMESTAMP) ? TIMESTAMP :
                             definicionCampo.includes(VARCHAR2) ? VARCHAR2 :
                             NUMBER;
                const valor = valores[indice]

                return {
                    campo,
                    tipo,
                    valor
                }
            })

            const query = `INSERT INTO ${nombreTabla} (${campos.join(', ')}) VALUES (${campoTipoValor.map(({ tipo, valor }) => {
                    if (tipo == NUMBER) {
                        return valor && valor != '' ? valor : 'NULL';

                    } else if (tipo == TIMESTAMP) {
                        return valor && valor != '' ? `TIMESTAMP \'${valor}\'` : 'NULL';

                    } else {
                        return valor && valor != '' ? `\'${valor}\'` : 'NULL';
                    }
                } )});`;

            inserts.push(query);
        })
        .on('end', () => {
            fs.writeFile(`${carpetaScripts}${nombreTabla}.sql`, inserts.join('\n'), (err) => {
                if (err) throw err;
                console.log(`SQL generado para ${nombreTabla}`);
            });
        });
}

fs.readdir(carpeta, (err, archivos) => {
    if (err) {
        return console.error('Error al leer la carpeta', err);
    }

    const tablasAProcesar = archivos
        .map(archivo => {
            let nombreTabla = archivo.split('/').pop().split('.')[0].split('_');
            nombreTabla = nombreTabla.splice(0, nombreTabla.length - 1).join('_');

            return { 
                rutaArchivo: `${carpeta}${archivo}`,
                nombreTabla
            };
        });

    armarMapaTablaColumnas();

    tablasAProcesar.forEach(({ rutaArchivo, nombreTabla }) => generarInserts(rutaArchivo, nombreTabla));

    generarDockerfile();
    generarShellScript();

    const used = process.memoryUsage().heapUsed / 1024 / 1024;
    
    console.log(`The script uses approximately ${Math.round(used * 100) / 100} MB`);
});

function generarShellScript() {
    const lineasShellScript = [];

    lineasShellScript.push("#!/bin/bash");
    lineasShellScript.push("docker build . -t oracle:latest");
    lineasShellScript.push("docker rm -f oracle");
    lineasShellScript.push("docker volume rm oracle-volume");
    lineasShellScript.push("docker run -d -p 1521:1521 -v oracle-volume:/opt/oracle/oradata -e ORACLE_PASSWORD=oracle --name oracle oracle");

    fs.writeFile(`${carpetaScripts}docker-run.sh`, lineasShellScript.join('\n'), (err) => {
        if (err) throw err;
        console.log(`Script de ejecución generado.`);
    });
}

function generarDockerfile() {
    const lineasDockerfile = [];
    const imagenBase = 'gvenzl/oracle-xe:18-slim';

    lineasDockerfile.push(`FROM ${imagenBase}`);
    lineasDockerfile.push('ARG dst="/container-entrypoint-initdb.d/"');

    fs.readdir(carpetaScripts, (err, archivosGenerados) => {
        const instruccionesAdd = archivosGenerados.map(archivo => `ADD ${archivo} $\{dst\}`);

        lineasDockerfile.push(...instruccionesAdd);

        fs.writeFile(`${carpetaScripts}Dockerfile`, lineasDockerfile.join('\n'), (err) => {
            if (err) throw err;
            console.log(`Dockerfile generado.`);
        });
    });

}

function armarMapaTablaColumnas() {
    const contenido = fs.readFileSync(rutaArchivoDDL, 'utf-8');
    const createTables = contenido.split(/\r?\n/);
    const FRASE_CREATE_TABLE = `CREATE TABLE "GESTOR"."`;

    let fraseEncontrada = false;
    let nuevaEntrada = undefined;

    for (linea of createTables) {
        if (!fraseEncontrada) {

            if (linea.includes(FRASE_CREATE_TABLE)) {
                fraseEncontrada = true;
                nuevaEntrada = {
                    nombreTabla: linea.substring(FRASE_CREATE_TABLE.length, linea.length - 2),
                    lineasCreate: [linea]
                };
                tablas.push(nuevaEntrada);
            }

        } else {
            nuevaEntrada['lineasCreate'].push(linea);
            
            if (linea.length > 1 && linea.substring(linea.length - 1) == ';') {
                fraseEncontrada = false;
            }
            
        }

    }

    //console.log("tablas", tablas[tablas.length - 1]);

}
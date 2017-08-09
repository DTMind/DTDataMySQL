<?php
/**
 * DTDataMySQL Class <http://www.dtmind.com>
 * Connection to db struture, form DTPDO
 * 
 * @version             version 1.00, 15/12/2016
 * @author		DTMind.com <develop@dtmind.com>
 * @author		Stefano Oggioni <stefano@oggioni.net>
 * @link 		https://github.com/DTMind/DTDataMySQL
 * @link 		http://www.dtmind.com/
 * @license		This software is licensed under the MIT license, http://opensource.org/licenses/MIT
 *
 */

class DTDataMySQL {

    static $dtFieldCounter=1;
    static $dtField="dtSqlField_";
    
    static function getRelations($dbh, $table) {

        $relationsDefinition=array();
        
        #if (!IsSet($this->joinIn[$table])) {

            // Si potrebbe estrapolare dalla struttura (relationsDefinition), ma si tratta di una sola query in più.
            $join = self::getJoin($dbh, $table, $database = '');
            $arr = $join;
            while (list($key, $value) = each($arr)) {

                if ($key != $table) {
                    $relationsDefinition[$key]['PRIMARY_KEYS'] = self::getPrimaryKeys($dbh, $key);
                    $relationsDefinition[$key]['JOIN'] = array($join[$key]['COLUMN_NAME'], $join[$key]['REFERENCED_TABLE_NAME'], $join[$key]['REFERENCED_COLUMN_NAME']);
                    $relationsDefinition[$key]['PARAMS'] = self::getParams($dbh, $key, $join[$key]['COLUMN_NAME'], $database);
                }
            }

        #}
        
        return array($table => $relationsDefinition);
    }
    

    static function getPrimaryKeys($dbh, $table, $database = "") {

        $res = array();

        $query = "SELECT `COLUMN_NAME`
                FROM 
                    `information_schema`.`COLUMNS` 
                WHERE 
                    " . (($database != "") ? " `TABLE_SCHEMA`='{$database}' AND " : "") . " 
                    `TABLE_NAME` ='{$table}' AND 
                    `COLUMN_KEY`='PRI'
                ORDER BY `TABLE_NAME`, `ORDINAL_POSITION`
                    ";

        $sth = $dbh->query($query);

        $sth->setFetchMode(PDO::FETCH_ASSOC);
        while ($row = $sth->fetch()) {
            $res[] = $row['COLUMN_NAME'];
        }

        return $res;
    }

    
    static function getParams($dbh, $table, $column, $database = '') {

        $res = array();

        $query = "SELECT `COLUMN_NAME`
                        FROM 
                            `information_schema`.`COLUMNS` 
                        WHERE 
                            " . (($database != "") ? " `TABLE_SCHEMA`='{$database}' AND " : "") . " 
                            `TABLE_NAME` ='{$table}' AND COLUMN_NAME<>'{$column}' AND
                            `COLUMN_KEY`='PRI'
                        ORDER BY `TABLE_NAME`, `ORDINAL_POSITION`
                            ";

                            
        $sth = $dbh->query($query);

        $sth->setFetchMode(PDO::FETCH_NUM);
        while ($row = $sth->fetch()) {
            # $res[$row[0]] = $row[0];
            $query ="SELECT `REFERENCED_TABLE_NAME`, `REFERENCED_COLUMN_NAME` ".
                        "FROM `information_schema`.`KEY_COLUMN_USAGE` ".
                        "WHERE ".
                            "`TABLE_NAME` = '{$table}' AND ".
                            "`COLUMN_NAME`='{$row[0]}' AND ".
                            "`REFERENCED_COLUMN_NAME` IS NOT NULL";

                            
            $connection = $dbh->getValues($query);
                        
            if (IsSet($connection)) {
                #die("--".$row[0]);
                $res[$row[0]] = $dbh->getValueList("SELECT {$connection["REFERENCED_COLUMN_NAME"]} FROM {$connection["REFERENCED_TABLE_NAME"]}");
                #myprint_r($res[$row[0]],1);
                
                }
            
            
        }

        return $res;
    }    
    
    static function getJoin($dbh, $table, $database = '') {

        $query = "SELECT `TABLE_NAME`, `COLUMN_NAME`, `REFERENCED_TABLE_NAME`, `REFERENCED_COLUMN_NAME`
                FROM `information_schema`.`KEY_COLUMN_USAGE`
                WHERE 
            " . (($database != "") ? " `TABLE_SCHEMA`='{$database}' AND " : "") . "
            `REFERENCED_TABLE_NAME`='{$table}' ";

        return $dbh->getValuesList($query);
    }    
    
    
    
    //////////////////////
    
    static function getObjectDefinitions($dbh, $relations, $fields, $database = '') {
        
        $table=key($relations);
        $relationsDefinition=$relations[$table];
        $fieldsDefinition = self::normilizeFields($table,$fields); 
        $objectDefinition=array();
        // Si potrebbe estrapolare dalla struttura (relationsDefinition), ma si tratta di una sola query in più.
        $join = self::getJoin($dbh, $table, $database = '');

        while (list($key, $value) = each($fieldsDefinition)) {
            if ($key != $table) {
                $objectDefinition[$key]=$relationsDefinition[$key];
                
            } else  {
                $objectDefinition[$key]['PRIMARY_KEY'] = self::getPrimaryKey($dbh, $table);;
            }
            $objectDefinition[$key]['FIELDS'] = self::getFields($dbh, $key, $value);
        }
        return $objectDefinition;
    }
    
    static function normilizeFields($table, $fields) {

        $res = array();

        // Normalizzation
        while (list(, $value) = each($fields)) {

            $tmp = explode(".", $value);
            if (!IsSet($tmp[1])) {
                $res[$table][$tmp[0]] = "";
            } else {
                $res[$tmp[0]][$tmp[1]] = "";
            }
        }

        return $res;
    }    

    static function getPrimaryKey($dbh, $table, $database = "") {

        $query = "SELECT `COLUMN_NAME`
                FROM 
                    `information_schema`.`COLUMNS` 
                WHERE 
                    " . (($database != "") ? " `TABLE_SCHEMA`='{$database}' AND " : "") . " 
                    `TABLE_NAME` ='{$table}' AND 
                    `COLUMN_KEY`='PRI'
                ORDER BY `TABLE_NAME`, `ORDINAL_POSITION`
                    ";

        return $dbh->getValue($query);
        ;
    }
    
    static function getFields($dbh, $table, $fields="*", $database = '') {

        
        $primaryKey=self::getPrimaryKey($dbh, $table);
        
        $res = array();
       
        $query = "SELECT `COLUMN_NAME`, `COLUMN_TYPE`, `EXTRA`, `IS_NULLABLE`, `CHARACTER_MAXIMUM_LENGTH`, `COLUMN_DEFAULT`
            FROM 
                `information_schema`.`COLUMNS` 
            WHERE 
                " . (($database != "") ? " `TABLE_SCHEMA`='{$database}' AND " : "") . "
                `TABLE_NAME` ='{$table}'
            ORDER BY `TABLE_NAME`, `ORDINAL_POSITION`
                ";

        $sth = $dbh->query($query);


        $sth->setFetchMode(PDO::FETCH_ASSOC);
        while ($row = $sth->fetch()) {

            #if (($this->allUpdateFields == 1) || (IsSet($this->object[$table][$row['COLUMN_NAME']]))) {
            if (($fields=="*") ||(IsSet($fields[$row['COLUMN_NAME']]))) {
                if ($fields=="*") {
                    $res[$row['COLUMN_NAME']] = self::getField($row,$primaryKey, 0);                
                } else {
                    $res[$row['COLUMN_NAME']] = self::getField($row, $primaryKey);
                }
            }
        }

        return $res;
    }

    // ---
    static function getField($row,$primaryKey, $field_name=1) {

        
        
        $infoType = self::parseField($row['COLUMN_TYPE']);

        $res = array();
        
        
        if ($primaryKey==$row['COLUMN_NAME']) {
            $res['PRIMARY_KEY']=1;
        }

        switch (strToUpper($infoType[0])) {
            //case "BOOL":
            //    $res['TYPE'] = "BOOL";
            //    $res['MIN'] = 0;
            //    $res['MAX'] = 1;
            //    break;



            case "TINYINT":
                $res['TYPE'] = "INT";
                if ($row['EXTRA'] == "auto_increment")
                    $res['AUTOINCREMENT'] = 1;
                if ($infoType[2] == "unsigned") {
                    $res['MIN'] = 0;
                    $res['MAX'] = 255;
                } else {
                    $res['MIN'] = -128;
                    $res['MAX'] = 127;
                }
                break;

            case "SMALLINT":
                $res['TYPE'] = "INT";
                if ($row['EXTRA'] == "auto_increment")
                    $res['AUTOINCREMENT'] = 1;
                if ($infoType[2] == "unsigned") {
                    $res['MIN'] = 0;
                    $res['MAX'] = 65535;
                } else {
                    $res['MIN'] = -32768;
                    $res['MAX'] = 32767;
                }
                break;

            case "MEDIUMINT":
                $res['TYPE'] = "INT";
                if ($row['EXTRA'] == "auto_increment")
                    $res['AUTOINCREMENT'] = 1;
                if ($infoType[2] == "unsigned") {
                    $res['MIN'] = 0;
                    $res['MAX'] = 16777215;
                } else {
                    $res['MIN'] = -8388608;
                    $res['MAX'] = 8388607;
                }
                break;

            case "INT":
            case "INTEGER":
                $res['TYPE'] = "INT";
                if ($row['EXTRA'] == "auto_increment")
                    $res['AUTOINCREMENT'] = 1;
                if ($infoType[2] == "unsigned") {
                    $res['MIN'] = 0;
                    $res['MAX'] = 4294967295;
                } else {
                    $res['MIN'] = -2147483648;
                    $res['MAX'] = 2147483647;
                }
                break;

            case "BIGINT":
                $res['TYPE'] = "INT";
                if ($row['EXTRA'] == "auto_increment")
                    $res['AUTOINCREMENT'] = 1;
                if ($infoType[2] == "unsigned") {
                    $res['MIN'] = 0;
                    $res['MAX'] = 18446744073709551615;
                } else {
                    $res['MIN'] = -9223372036854775808;
                    $res['MAX'] = 9223372036854775807;
                }
                break;

            case "DECIMAL":
            case "DEC":
            case "NUMERIC":
            case "FIXED":
                $res['TYPE'] = "REAL";
                break;
            case "FLOAT":
                $res['TYPE'] = "REAL";
                break;
            case "DOUBLE":
                $res['TYPE'] = "REAL";
                break;

            case "DATE":
                $res['TYPE'] = "DATE";
                break;

            case "DATETIME":
                $res['TYPE'] = "DATETIME";
                break;

            case "TIME":
                $res['TYPE'] = "TIME";
                break;

            case "CHAR":
            case "VARCHAR":
            case "TINYTEXT":
            case "TEXT":
            case "MEDIUMTEXT":
            case "LONGTEXT":
                $res['TYPE'] = "TEXT";
                if ($row['IS_NULLABLE'] == "NO") {
                    $res['MIN_LENGTH'] = 1;
                } else {
                    $res['MIN_LENGTH'] = 0;
                }
                $res['MAX_LENGTH'] = $row['CHARACTER_MAXIMUM_LENGTH'];
                break;

            case "ENUM":
                $res['TYPE'] = "ENUM";
                $res['VALUE_LIST'] = $infoType[1];
                break;

            case "SET":
                $res['TYPE'] = "SET";
                $res['VALUE_LIST'] = $infoType[1];
                break;

            default:
                die("NON PREVISTO: " . $infoType[0]);
        }

        // Per tutti
        if ($row['IS_NULLABLE'] == "NO") {
            $res['REQUIRED'] = 1;
        } else {
            $res['REQUIRED'] = 0;
        }

        $res['DEFAULT_VALUE'] = $row['COLUMN_DEFAULT'];

        if ($field_name==1) {
            $res['FIELD_NAME'] = self::$dtField . self::$dtFieldCounter;
        }
        self::$dtFieldCounter++;

        return $res;
    }

    
    
    static function parseField($value) {

        $res = array();

        $tmp = explode(" ", $value);
        if (IsSet($tmp[1])) {
            $res[2] = $tmp[1];
        } else {
            $res[2] = "";
        }


        $tmp = explode("(", $tmp[0]);
        if (IsSet($tmp[1])) {
            $res[0] = $tmp[0];
            $res[1] = substr($tmp[1], 0, strlen($tmp[1]) - 1);
        } else {
            $res[0] = $tmp[0];
            $res[1] = "";
        }

        return $res;
    }    
    
    static function overrideObject($objectDefinition, $override) {

        $primaryTable=key($objectDefinition);
        
        while (list($key1, $value1) = each($override)) {
            
            $info=explode(".",$key1);
            if (IsSet($info[1])) {
                $table=$info[0];
                $field=$info[1];
            } else {
                $table=$primaryTable;
                $field=$info[0];        
            }
            
            while (list($key2, $value2) = each($value1)) {                
                    $objectDefinition[$table]['FIELDS'][$field][$key2] = $value2;
                #}
            }
        }
    

        return $objectDefinition;
        
    }
    
    function getAllKeys($dbh) {

        $res = array();

        $query = "SELECT  `TABLE_NAME`,`COLUMN_NAME`, `COLUMN_KEY`, `EXTRA` 
                FROM 
                    `information_schema`.`COLUMNS` 
                WHERE 
                    `COLUMN_KEY`<>''
                    ";

        $sth = $dbh->query($query);

        $sth->setFetchMode(PDO::FETCH_ASSOC);
        while ($row = $sth->fetch()) {
            $res[$row['TABLE_NAME'].".".$row['COLUMN_NAME']] = array($row['COLUMN_KEY'],$row['EXTRA']);
        }

        return $res;

        
    }
    
}

?>
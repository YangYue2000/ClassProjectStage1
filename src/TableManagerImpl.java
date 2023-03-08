import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;

import com.apple.foundationdb.*;
import com.apple.foundationdb.async.AsyncIterable;
import com.apple.foundationdb.directory.DirectoryLayer;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.directory.PathUtil;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.ByteArrayUtil;
import com.apple.foundationdb.tuple.Tuple;
/**
 * TableManagerImpl implements interfaces in {#TableManager}. You should put your implementation
 * in this class.
 */
public class TableManagerImpl implements TableManager{

  @Override
  public StatusCode createTable(String tableName, String[] attributeNames, AttributeType[] attributeType,
                         String[] primaryKeyAttributeNames) {
    if(attributeNames==null||attributeType==null||attributeNames.length!=attributeType.length){
      return StatusCode.TABLE_CREATION_ATTRIBUTE_INVALID;
    }
    if(primaryKeyAttributeNames==null) return StatusCode.TABLE_CREATION_NO_PRIMARY_KEY;
    boolean found = false;
    for (String primaryKeyAttribute : primaryKeyAttributeNames) {
      for (String attributeName : attributeNames) {
        if (primaryKeyAttribute.equals(attributeName)) {
          found = true;
          break;
        }
      }
      if (!found) break;
    }
    if(!found) return StatusCode.TABLE_CREATION_PRIMARY_KEY_NOT_FOUND;
    // your code
    FDB fdb = FDB.selectAPIVersion(710);
    Database db = null;
    DirectorySubspace rootDirectory = null;
    try {
      db = fdb.open();
    } catch (Exception e) {
      System.out.println("ERROR: the database is not successfully opened: " + e);
    }
    try {
      rootDirectory = DirectoryLayer.getDefault().createOrOpen(db,
              PathUtil.from("rootTable")).join();
    } catch (Exception e) {
      System.out.println("ERROR: the root directory is not successfully opened: " + e);
    }
    Transaction tx = db.createTransaction();
    final DirectorySubspace table = rootDirectory.createOrOpen(db, PathUtil.from(tableName)).join();
    for(int i=0; i<attributeNames.length; i++){
      int isPrim = 0;
      for(String pk : primaryKeyAttributeNames){
        if(attributeNames[i].equals(pk)){
          isPrim = 1;
          break;
        }
      }
      Tuple keyTuple = new Tuple();
      keyTuple = keyTuple.add(attributeNames[i]);
      Tuple valueTuple = new Tuple();
      int type = -1;
      if(attributeType[i]==AttributeType.INT) type = 1;
      else if(attributeType[i]==AttributeType.VARCHAR) type = 2;
      else if(attributeType[i]==AttributeType.DOUBLE) type = 3;
      valueTuple = valueTuple.add(type).add(isPrim);
      tx.set(table.pack(keyTuple), valueTuple.pack());
    }
    tx.commit().join();
    tx.close();
    db.close();
    return StatusCode.SUCCESS;
  }

  @Override
  public StatusCode deleteTable(String tableName) {
    // your code
    FDB fdb = FDB.selectAPIVersion(710);
    Database db = null;
    DirectorySubspace rootDirectory = null;
    try {
      db = fdb.open();
    } catch (Exception e) {
      System.out.println("ERROR: the database is not successfully opened: " + e);
    }
    try {
      rootDirectory = DirectoryLayer.getDefault().createOrOpen(db,
              PathUtil.from("rootTable")).join();
    } catch (Exception e) {
      System.out.println("ERROR: the root directory is not successfully opened: " + e);
    }
    List<String> subspacePath = rootDirectory.list(db).join();
    for(String name : subspacePath){
      if(name.equals(tableName)){
        DirectorySubspace table = rootDirectory.createOrOpen(db, PathUtil.from(tableName)).join();
        db.run(tx -> {
          tx.clear(table.range());
          return null;
        });
        db.close();
        return StatusCode.SUCCESS;
      }
    }
    db.close();
    return StatusCode.TABLE_NOT_FOUND;
  }

  @Override
  public HashMap<String, TableMetadata> listTables() {
    HashMap<String, TableMetadata> map = new HashMap<>();
    // your code
    FDB fdb = FDB.selectAPIVersion(710);
    Database db = null;
    DirectorySubspace rootDirectory = null;
    try {
      db = fdb.open();
    } catch (Exception e) {
      System.out.println("ERROR: the database is not successfully opened: " + e);
    }
    try {
      rootDirectory = DirectoryLayer.getDefault().createOrOpen(db,
              PathUtil.from("rootTable")).join();
    } catch (Exception e) {
      System.out.println("ERROR: the root directory is not successfully opened: " + e);
    }
    Transaction tx = db.createTransaction();
    List<String> subspacePath = rootDirectory.list(db).join();
    for(String tableName : subspacePath){
      List<String> primaryKeys = new ArrayList<>();
      TableMetadata meta = new TableMetadata();
      final DirectorySubspace table = rootDirectory.createOrOpen(db, PathUtil.from(tableName)).join();
      Range range = table.range();
// Get all key-value pairs in the range
      AsyncIterable<KeyValue> keyValues = tx.getRange(range);
// Iterate through the key-value pairs to get all the attributes
      for (KeyValue keyValue : keyValues) {
        byte[] key = keyValue.getKey();
        byte[] value = keyValue.getValue();
        // Convert the byte arrays to tuples
        Tuple keyTuple = Tuple.fromBytes(key);
        Tuple valueTuple = Tuple.fromBytes(value);
        String attributeName = (String) keyTuple.get(1);
        // Get the attribute from the value tuple
        long typeINT = (long) valueTuple.get(0);
        AttributeType type = null;
        if(typeINT==1) type = AttributeType.INT;
        else if(typeINT==2) type = AttributeType.VARCHAR;
        meta.addAttribute(attributeName,type);
        if((long)valueTuple.get(1)==1){
          primaryKeys.add(attributeName);
        }
        // Print the attribute
      }
      if(!primaryKeys.isEmpty()) {
        if(primaryKeys.size()==2){
          String temp = primaryKeys.get(0);
          primaryKeys.set(0,primaryKeys.get(1));
          primaryKeys.set(1,temp);
        }
        meta.setPrimaryKeys(primaryKeys);
        map.put(tableName, meta);
      }
    }
    tx.close();
    db.close();
    return map;
  }

  @Override
  public StatusCode addAttribute(String tableName, String attributeName, AttributeType attributeType) {
    // your code
    FDB fdb = FDB.selectAPIVersion(710);
    Database db = null;
    DirectorySubspace rootDirectory = null;
    try {
      db = fdb.open();
    } catch (Exception e) {
      System.out.println("ERROR: the database is not successfully opened: " + e);
    }
    try {
      rootDirectory = DirectoryLayer.getDefault().createOrOpen(db,
              PathUtil.from("rootTable")).join();
    } catch (Exception e) {
      System.out.println("ERROR: the root directory is not successfully opened: " + e);
    }
    Transaction tx = db.createTransaction();
    List<String> subspacePath = rootDirectory.list(db).join();
    for(String name : subspacePath){
      if(name.equals(tableName)){
        final DirectorySubspace table = rootDirectory.createOrOpen(db, PathUtil.from(tableName)).join();
        Range range = table.range();
// Get all key-value pairs in the range
        AsyncIterable<KeyValue> keyValues = tx.getRange(range);
// Iterate through the key-value pairs to get all the attributes
        for (KeyValue keyValue : keyValues) {
          byte[] key = keyValue.getKey();
          // Convert the byte arrays to tuples
          Tuple keyTuple = Tuple.fromBytes(key);
          String attrName = (String) keyTuple.get(1);
          if (attrName.equals(attributeName)) {
            tx.close();
            db.close();
            return StatusCode.ATTRIBUTE_ALREADY_EXISTS;
          }
        }
        Tuple keyTuple = new Tuple();
        keyTuple = keyTuple.add(attributeName);
        Tuple valueTuple = new Tuple();
        int type = -1;
        if(attributeType==AttributeType.INT) type = 1;
        else if(attributeType==AttributeType.VARCHAR) type = 2;
        else if(attributeType==AttributeType.DOUBLE) type = 3;
        valueTuple = valueTuple.add(type).add(0);
        tx.set(table.pack(keyTuple), valueTuple.pack());
        tx.commit().join();
        tx.close();
        db.close();
        return StatusCode.SUCCESS;
      }
    }
    tx.close();
    db.close();
    return StatusCode.TABLE_NOT_FOUND;
  }

  @Override
  public StatusCode dropAttribute(String tableName, String attributeName) {
    // your code
    FDB fdb = FDB.selectAPIVersion(710);
    Database db = null;
    DirectorySubspace rootDirectory = null;
    try {
      db = fdb.open();
    } catch (Exception e) {
      System.out.println("ERROR: the database is not successfully opened: " + e);
    }
    try {
      rootDirectory = DirectoryLayer.getDefault().createOrOpen(db,
              PathUtil.from("rootTable")).join();
    } catch (Exception e) {
      System.out.println("ERROR: the root directory is not successfully opened: " + e);
    }
    Transaction tx = db.createTransaction();
    List<String> subspacePath = rootDirectory.list(db).join();
    for(String name : subspacePath) {
      if (name.equals(tableName)) {
        final DirectorySubspace table = rootDirectory.createOrOpen(db, PathUtil.from(tableName)).join();
        Range range = table.range();
// Get all key-value pairs in the range
        AsyncIterable<KeyValue> keyValues = tx.getRange(range);
// Iterate through the key-value pairs to get all the attributes
        for (KeyValue keyValue : keyValues) {
          byte[] key = keyValue.getKey();
          // Convert the byte arrays to tuples
          Tuple keyTuple = Tuple.fromBytes(key);
          String attrName = (String) keyTuple.get(1);
          if (attrName.equals(attributeName)) {
            tx.clear(key);
            tx.commit().join();
            tx.close();
            db.close();
            return StatusCode.SUCCESS;
          }
        }
        db.close();
        tx.close();
        return StatusCode.ATTRIBUTE_NOT_FOUND;
      }
    }
    db.close();
    tx.close();
    return StatusCode.TABLE_NOT_FOUND;
  }

  @Override
  public StatusCode dropAllTables() {
    // your code
    FDB fdb = FDB.selectAPIVersion(710);
    Database db = null;
    try {
      db = fdb.open();
    } catch (Exception e) {
      System.out.println("ERROR: the database is not successfully opened: " + e);
    }
    db.run(tx -> {
      final byte[] st = new Subspace(new byte[]{(byte) 0x00}).getKey();
      final byte[] en = new Subspace(new byte[]{(byte) 0xFF}).getKey();
      tx.clear(st, en);
      return null;
    });
    db.close();
    return StatusCode.SUCCESS;
  }
}

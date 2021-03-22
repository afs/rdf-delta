/*
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 *  See the NOTICE file distributed with this work for additional
 *  information regarding copyright ownership.
 */

package org.seaborne.patch.binary.thrift;

import org.apache.thrift.protocol.TProtocolException;

import java.util.Map;
import java.util.HashMap;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.Collections;

@SuppressWarnings("all")
public class RDF_Patch_Row extends org.apache.thrift.TUnion<RDF_Patch_Row, RDF_Patch_Row._Fields> {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("RDF_Patch_Row");
  private static final org.apache.thrift.protocol.TField HEADER_FIELD_DESC = new org.apache.thrift.protocol.TField("header", org.apache.thrift.protocol.TType.STRUCT, (short)1);
  private static final org.apache.thrift.protocol.TField DATA_ADD_FIELD_DESC = new org.apache.thrift.protocol.TField("dataAdd", org.apache.thrift.protocol.TType.STRUCT, (short)2);
  private static final org.apache.thrift.protocol.TField DATA_DEL_FIELD_DESC = new org.apache.thrift.protocol.TField("dataDel", org.apache.thrift.protocol.TType.STRUCT, (short)3);
  private static final org.apache.thrift.protocol.TField PREFIX_ADD_FIELD_DESC = new org.apache.thrift.protocol.TField("prefixAdd", org.apache.thrift.protocol.TType.STRUCT, (short)4);
  private static final org.apache.thrift.protocol.TField PREFIX_DEL_FIELD_DESC = new org.apache.thrift.protocol.TField("prefixDel", org.apache.thrift.protocol.TType.STRUCT, (short)5);
  private static final org.apache.thrift.protocol.TField TXN_FIELD_DESC = new org.apache.thrift.protocol.TField("txn", org.apache.thrift.protocol.TType.I32, (short)6);

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    HEADER((short)1, "header"),
    DATA_ADD((short)2, "dataAdd"),
    DATA_DEL((short)3, "dataDel"),
    PREFIX_ADD((short)4, "prefixAdd"),
    PREFIX_DEL((short)5, "prefixDel"),
    /**
     * 
     * @see Transaction
     */
    TXN((short)6, "txn");

    private static final Map<String, _Fields> byName = new HashMap<String, _Fields>();

    static {
      for (_Fields field : EnumSet.allOf(_Fields.class)) {
        byName.put(field.getFieldName(), field);
      }
    }

    /**
     * Find the _Fields constant that matches fieldId, or null if its not found.
     */
    public static _Fields findByThriftId(int fieldId) {
      switch(fieldId) {
        case 1: // HEADER
          return HEADER;
        case 2: // DATA_ADD
          return DATA_ADD;
        case 3: // DATA_DEL
          return DATA_DEL;
        case 4: // PREFIX_ADD
          return PREFIX_ADD;
        case 5: // PREFIX_DEL
          return PREFIX_DEL;
        case 6: // TXN
          return TXN;
        default:
          return null;
      }
    }

    /**
     * Find the _Fields constant that matches fieldId, throwing an exception
     * if it is not found.
     */
    public static _Fields findByThriftIdOrThrow(int fieldId) {
      _Fields fields = findByThriftId(fieldId);
      if (fields == null) throw new IllegalArgumentException("Field " + fieldId + " doesn't exist!");
      return fields;
    }

    /**
     * Find the _Fields constant that matches name, or null if its not found.
     */
    public static _Fields findByName(String name) {
      return byName.get(name);
    }

    private final short _thriftId;
    private final String _fieldName;

    _Fields(short thriftId, String fieldName) {
      _thriftId = thriftId;
      _fieldName = fieldName;
    }

    public short getThriftFieldId() {
      return _thriftId;
    }

    public String getFieldName() {
      return _fieldName;
    }
  }

  public static final Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
  static {
    Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(_Fields.class);
    tmpMap.put(_Fields.HEADER, new org.apache.thrift.meta_data.FieldMetaData("header", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.StructMetaData(org.apache.thrift.protocol.TType.STRUCT, Patch_Header.class)));
    tmpMap.put(_Fields.DATA_ADD, new org.apache.thrift.meta_data.FieldMetaData("dataAdd", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.StructMetaData(org.apache.thrift.protocol.TType.STRUCT, Patch_Data_Add.class)));
    tmpMap.put(_Fields.DATA_DEL, new org.apache.thrift.meta_data.FieldMetaData("dataDel", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.StructMetaData(org.apache.thrift.protocol.TType.STRUCT, Patch_Data_Del.class)));
    tmpMap.put(_Fields.PREFIX_ADD, new org.apache.thrift.meta_data.FieldMetaData("prefixAdd", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.StructMetaData(org.apache.thrift.protocol.TType.STRUCT, Patch_Prefix_Add.class)));
    tmpMap.put(_Fields.PREFIX_DEL, new org.apache.thrift.meta_data.FieldMetaData("prefixDel", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.StructMetaData(org.apache.thrift.protocol.TType.STRUCT, Patch_Prefix_Del.class)));
    tmpMap.put(_Fields.TXN, new org.apache.thrift.meta_data.FieldMetaData("txn", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.EnumMetaData(org.apache.thrift.protocol.TType.ENUM, Transaction.class)));
    metaDataMap = Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(RDF_Patch_Row.class, metaDataMap);
  }

  public RDF_Patch_Row() {
    super();
  }

  public RDF_Patch_Row(_Fields setField, Object value) {
    super(setField, value);
  }

  public RDF_Patch_Row(RDF_Patch_Row other) {
    super(other);
  }
  public RDF_Patch_Row deepCopy() {
    return new RDF_Patch_Row(this);
  }

  public static RDF_Patch_Row header(Patch_Header value) {
    RDF_Patch_Row x = new RDF_Patch_Row();
    x.setHeader(value);
    return x;
  }

  public static RDF_Patch_Row dataAdd(Patch_Data_Add value) {
    RDF_Patch_Row x = new RDF_Patch_Row();
    x.setDataAdd(value);
    return x;
  }

  public static RDF_Patch_Row dataDel(Patch_Data_Del value) {
    RDF_Patch_Row x = new RDF_Patch_Row();
    x.setDataDel(value);
    return x;
  }

  public static RDF_Patch_Row prefixAdd(Patch_Prefix_Add value) {
    RDF_Patch_Row x = new RDF_Patch_Row();
    x.setPrefixAdd(value);
    return x;
  }

  public static RDF_Patch_Row prefixDel(Patch_Prefix_Del value) {
    RDF_Patch_Row x = new RDF_Patch_Row();
    x.setPrefixDel(value);
    return x;
  }

  public static RDF_Patch_Row txn(Transaction value) {
    RDF_Patch_Row x = new RDF_Patch_Row();
    x.setTxn(value);
    return x;
  }


  @Override
  protected void checkType(_Fields setField, Object value) throws ClassCastException {
    switch (setField) {
      case HEADER:
        if (value instanceof Patch_Header) {
          break;
        }
        throw new ClassCastException("Was expecting value of type Patch_Header for field 'header', but got " + value.getClass().getSimpleName());
      case DATA_ADD:
        if (value instanceof Patch_Data_Add) {
          break;
        }
        throw new ClassCastException("Was expecting value of type Patch_Data_Add for field 'dataAdd', but got " + value.getClass().getSimpleName());
      case DATA_DEL:
        if (value instanceof Patch_Data_Del) {
          break;
        }
        throw new ClassCastException("Was expecting value of type Patch_Data_Del for field 'dataDel', but got " + value.getClass().getSimpleName());
      case PREFIX_ADD:
        if (value instanceof Patch_Prefix_Add) {
          break;
        }
        throw new ClassCastException("Was expecting value of type Patch_Prefix_Add for field 'prefixAdd', but got " + value.getClass().getSimpleName());
      case PREFIX_DEL:
        if (value instanceof Patch_Prefix_Del) {
          break;
        }
        throw new ClassCastException("Was expecting value of type Patch_Prefix_Del for field 'prefixDel', but got " + value.getClass().getSimpleName());
      case TXN:
        if (value instanceof Transaction) {
          break;
        }
        throw new ClassCastException("Was expecting value of type Transaction for field 'txn', but got " + value.getClass().getSimpleName());
      default:
        throw new IllegalArgumentException("Unknown field id " + setField);
    }
  }

  @Override
  protected Object standardSchemeReadValue(org.apache.thrift.protocol.TProtocol iprot, org.apache.thrift.protocol.TField field) throws org.apache.thrift.TException {
    _Fields setField = _Fields.findByThriftId(field.id);
    if (setField != null) {
      switch (setField) {
        case HEADER:
          if (field.type == HEADER_FIELD_DESC.type) {
            Patch_Header header;
            header = new Patch_Header();
            header.read(iprot);
            return header;
          } else {
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot, field.type);
            return null;
          }
        case DATA_ADD:
          if (field.type == DATA_ADD_FIELD_DESC.type) {
            Patch_Data_Add dataAdd;
            dataAdd = new Patch_Data_Add();
            dataAdd.read(iprot);
            return dataAdd;
          } else {
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot, field.type);
            return null;
          }
        case DATA_DEL:
          if (field.type == DATA_DEL_FIELD_DESC.type) {
            Patch_Data_Del dataDel;
            dataDel = new Patch_Data_Del();
            dataDel.read(iprot);
            return dataDel;
          } else {
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot, field.type);
            return null;
          }
        case PREFIX_ADD:
          if (field.type == PREFIX_ADD_FIELD_DESC.type) {
            Patch_Prefix_Add prefixAdd;
            prefixAdd = new Patch_Prefix_Add();
            prefixAdd.read(iprot);
            return prefixAdd;
          } else {
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot, field.type);
            return null;
          }
        case PREFIX_DEL:
          if (field.type == PREFIX_DEL_FIELD_DESC.type) {
            Patch_Prefix_Del prefixDel;
            prefixDel = new Patch_Prefix_Del();
            prefixDel.read(iprot);
            return prefixDel;
          } else {
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot, field.type);
            return null;
          }
        case TXN:
          if (field.type == TXN_FIELD_DESC.type) {
            Transaction txn;
            txn = Transaction.findByValue(iprot.readI32());
            return txn;
          } else {
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot, field.type);
            return null;
          }
        default:
          throw new IllegalStateException("setField wasn't null, but didn't match any of the case statements!");
      }
    } else {
      org.apache.thrift.protocol.TProtocolUtil.skip(iprot, field.type);
      return null;
    }
  }

  @Override
  protected void standardSchemeWriteValue(org.apache.thrift.protocol.TProtocol oprot) throws org.apache.thrift.TException {
    switch (setField_) {
      case HEADER:
        Patch_Header header = (Patch_Header)value_;
        header.write(oprot);
        return;
      case DATA_ADD:
        Patch_Data_Add dataAdd = (Patch_Data_Add)value_;
        dataAdd.write(oprot);
        return;
      case DATA_DEL:
        Patch_Data_Del dataDel = (Patch_Data_Del)value_;
        dataDel.write(oprot);
        return;
      case PREFIX_ADD:
        Patch_Prefix_Add prefixAdd = (Patch_Prefix_Add)value_;
        prefixAdd.write(oprot);
        return;
      case PREFIX_DEL:
        Patch_Prefix_Del prefixDel = (Patch_Prefix_Del)value_;
        prefixDel.write(oprot);
        return;
      case TXN:
        Transaction txn = (Transaction)value_;
        oprot.writeI32(txn.getValue());
        return;
      default:
        throw new IllegalStateException("Cannot write union with unknown field " + setField_);
    }
  }

  @Override
  protected Object tupleSchemeReadValue(org.apache.thrift.protocol.TProtocol iprot, short fieldID) throws org.apache.thrift.TException {
    _Fields setField = _Fields.findByThriftId(fieldID);
    if (setField != null) {
      switch (setField) {
        case HEADER:
          Patch_Header header;
          header = new Patch_Header();
          header.read(iprot);
          return header;
        case DATA_ADD:
          Patch_Data_Add dataAdd;
          dataAdd = new Patch_Data_Add();
          dataAdd.read(iprot);
          return dataAdd;
        case DATA_DEL:
          Patch_Data_Del dataDel;
          dataDel = new Patch_Data_Del();
          dataDel.read(iprot);
          return dataDel;
        case PREFIX_ADD:
          Patch_Prefix_Add prefixAdd;
          prefixAdd = new Patch_Prefix_Add();
          prefixAdd.read(iprot);
          return prefixAdd;
        case PREFIX_DEL:
          Patch_Prefix_Del prefixDel;
          prefixDel = new Patch_Prefix_Del();
          prefixDel.read(iprot);
          return prefixDel;
        case TXN:
          Transaction txn;
          txn = Transaction.findByValue(iprot.readI32());
          return txn;
        default:
          throw new IllegalStateException("setField wasn't null, but didn't match any of the case statements!");
      }
    } else {
      throw new TProtocolException("Couldn't find a field with field id " + fieldID);
    }
  }

  @Override
  protected void tupleSchemeWriteValue(org.apache.thrift.protocol.TProtocol oprot) throws org.apache.thrift.TException {
    switch (setField_) {
      case HEADER:
        Patch_Header header = (Patch_Header)value_;
        header.write(oprot);
        return;
      case DATA_ADD:
        Patch_Data_Add dataAdd = (Patch_Data_Add)value_;
        dataAdd.write(oprot);
        return;
      case DATA_DEL:
        Patch_Data_Del dataDel = (Patch_Data_Del)value_;
        dataDel.write(oprot);
        return;
      case PREFIX_ADD:
        Patch_Prefix_Add prefixAdd = (Patch_Prefix_Add)value_;
        prefixAdd.write(oprot);
        return;
      case PREFIX_DEL:
        Patch_Prefix_Del prefixDel = (Patch_Prefix_Del)value_;
        prefixDel.write(oprot);
        return;
      case TXN:
        Transaction txn = (Transaction)value_;
        oprot.writeI32(txn.getValue());
        return;
      default:
        throw new IllegalStateException("Cannot write union with unknown field " + setField_);
    }
  }

  @Override
  protected org.apache.thrift.protocol.TField getFieldDesc(_Fields setField) {
    switch (setField) {
      case HEADER:
        return HEADER_FIELD_DESC;
      case DATA_ADD:
        return DATA_ADD_FIELD_DESC;
      case DATA_DEL:
        return DATA_DEL_FIELD_DESC;
      case PREFIX_ADD:
        return PREFIX_ADD_FIELD_DESC;
      case PREFIX_DEL:
        return PREFIX_DEL_FIELD_DESC;
      case TXN:
        return TXN_FIELD_DESC;
      default:
        throw new IllegalArgumentException("Unknown field id " + setField);
    }
  }

  @Override
  protected org.apache.thrift.protocol.TStruct getStructDesc() {
    return STRUCT_DESC;
  }

  @Override
  protected _Fields enumForId(short id) {
    return _Fields.findByThriftIdOrThrow(id);
  }

  public _Fields fieldForId(int fieldId) {
    return _Fields.findByThriftId(fieldId);
  }


  public Patch_Header getHeader() {
    if (getSetField() == _Fields.HEADER) {
      return (Patch_Header)getFieldValue();
    } else {
      throw new RuntimeException("Cannot get field 'header' because union is currently set to " + getFieldDesc(getSetField()).name);
    }
  }

  public void setHeader(Patch_Header value) {
    if (value == null) throw new NullPointerException();
    setField_ = _Fields.HEADER;
    value_ = value;
  }

  public Patch_Data_Add getDataAdd() {
    if (getSetField() == _Fields.DATA_ADD) {
      return (Patch_Data_Add)getFieldValue();
    } else {
      throw new RuntimeException("Cannot get field 'dataAdd' because union is currently set to " + getFieldDesc(getSetField()).name);
    }
  }

  public void setDataAdd(Patch_Data_Add value) {
    if (value == null) throw new NullPointerException();
    setField_ = _Fields.DATA_ADD;
    value_ = value;
  }

  public Patch_Data_Del getDataDel() {
    if (getSetField() == _Fields.DATA_DEL) {
      return (Patch_Data_Del)getFieldValue();
    } else {
      throw new RuntimeException("Cannot get field 'dataDel' because union is currently set to " + getFieldDesc(getSetField()).name);
    }
  }

  public void setDataDel(Patch_Data_Del value) {
    if (value == null) throw new NullPointerException();
    setField_ = _Fields.DATA_DEL;
    value_ = value;
  }

  public Patch_Prefix_Add getPrefixAdd() {
    if (getSetField() == _Fields.PREFIX_ADD) {
      return (Patch_Prefix_Add)getFieldValue();
    } else {
      throw new RuntimeException("Cannot get field 'prefixAdd' because union is currently set to " + getFieldDesc(getSetField()).name);
    }
  }

  public void setPrefixAdd(Patch_Prefix_Add value) {
    if (value == null) throw new NullPointerException();
    setField_ = _Fields.PREFIX_ADD;
    value_ = value;
  }

  public Patch_Prefix_Del getPrefixDel() {
    if (getSetField() == _Fields.PREFIX_DEL) {
      return (Patch_Prefix_Del)getFieldValue();
    } else {
      throw new RuntimeException("Cannot get field 'prefixDel' because union is currently set to " + getFieldDesc(getSetField()).name);
    }
  }

  public void setPrefixDel(Patch_Prefix_Del value) {
    if (value == null) throw new NullPointerException();
    setField_ = _Fields.PREFIX_DEL;
    value_ = value;
  }

  /**
   * 
   * @see Transaction
   */
  public Transaction getTxn() {
    if (getSetField() == _Fields.TXN) {
      return (Transaction)getFieldValue();
    } else {
      throw new RuntimeException("Cannot get field 'txn' because union is currently set to " + getFieldDesc(getSetField()).name);
    }
  }

  /**
   * 
   * @see Transaction
   */
  public void setTxn(Transaction value) {
    if (value == null) throw new NullPointerException();
    setField_ = _Fields.TXN;
    value_ = value;
  }

  public boolean isSetHeader() {
    return setField_ == _Fields.HEADER;
  }


  public boolean isSetDataAdd() {
    return setField_ == _Fields.DATA_ADD;
  }


  public boolean isSetDataDel() {
    return setField_ == _Fields.DATA_DEL;
  }


  public boolean isSetPrefixAdd() {
    return setField_ == _Fields.PREFIX_ADD;
  }


  public boolean isSetPrefixDel() {
    return setField_ == _Fields.PREFIX_DEL;
  }


  public boolean isSetTxn() {
    return setField_ == _Fields.TXN;
  }


  public boolean equals(Object other) {
    if (other instanceof RDF_Patch_Row) {
      return equals((RDF_Patch_Row)other);
    } else {
      return false;
    }
  }

  public boolean equals(RDF_Patch_Row other) {
    return other != null && getSetField() == other.getSetField() && getFieldValue().equals(other.getFieldValue());
  }

  @Override
  public int compareTo(RDF_Patch_Row other) {
    int lastComparison = org.apache.thrift.TBaseHelper.compareTo(getSetField(), other.getSetField());
    if (lastComparison == 0) {
      return org.apache.thrift.TBaseHelper.compareTo(getFieldValue(), other.getFieldValue());
    }
    return lastComparison;
  }


  /**
   * If you'd like this to perform more respectably, use the hashcode generator option.
   */
  @Override
  public int hashCode() {
    return 0;
  }

  private void writeObject(java.io.ObjectOutputStream out) throws java.io.IOException {
    try {
      write(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(out)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }


  private void readObject(java.io.ObjectInputStream in) throws java.io.IOException, ClassNotFoundException {
    try {
      read(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(in)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }


}

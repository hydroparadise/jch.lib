package jch.lib.cloud.gcp.bigquery;

import java.util.ArrayList;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.*;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.JobException;
import com.google.cloud.bigquery.QueryJobConfiguration;

import jch.lib.log.QLog;

/***
 * 
 * @author ChadHarrison
 *
 */
public abstract class BigQueryHelper {

	public enum ObjectType{
		OBJECT,
		PROJECT,
		DATASET,
		TABLE,
		FIELD
	}
	
	public enum DataType {
		ARRAY,
		BIGNUMERIC,
		BOOL,
		BYTES,
		DATE,
		DATETIME,
		FLOAT64,
		FLOAT,
		GEOGRAPHY,
		INT64,
		INT,
		INTERVAL,
		JSON,
		NUMERIC,
		STRING,
		STRUCT,
		TIME,
		TIMESTAMP
	}

	
	public interface SafeNameable {
		public String getName();
		public String getSafeName();
		public String getFullSafeName();
		public ObjectType getObjectType();

	}
	
	
	/***
	 * Used to get around null exception when attempting to pull from BigQuery FieldValue 
	 * @param fvl
	 * @param fieldName
	 * @return
	 */
	public static String extractString(FieldValueList fvl, String fieldName) {
		String output = (fvl.get(fieldName).isNull()) ? "" : fvl.get(fieldName).getStringValue();
		return output;
	}
	
	/***
	 * Returns a qulified name
	 * Example: table -> `table`
	 * @param bqObjectName
	 * @return
	 */
	public static String getSafeName(String bqObjectName) {
		if(bqObjectName == "" || bqObjectName == null)
			return "";
		//make sure object name isn't already qualified
		else if(bqObjectName.charAt(0) == '`' && bqObjectName.charAt(bqObjectName.length() -1) == '`')
			return bqObjectName;
		else 
			return "`" + bqObjectName + "`";
	}
	
	/***
	 * Turns ArrayList of FieldAttributes and converts to ArrayList of Field
	 * @param tbl
	 * @param bigquery
	 * @return
	 */
	public static ArrayList<BigQueryHelper.Field> fillFields(BigQueryHelper.Table tbl, BigQuery bigquery) {
		ArrayList<BigQueryHelper.Field> output = new ArrayList<BigQueryHelper.Field>();
		
		try {
			
			String sqlFlds = BigQueryDiscovery.sqlColumnInformationShema(tbl.getFullSafeDataSetName(), tbl.getName());
			QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(sqlFlds).build();
			QLog.log(sqlFlds);
			
			//throws InterruptedException
			for (FieldValueList row : bigquery.query(queryConfig).iterateAll()) {
				
				/*
			      + TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION, IS_NULLABLE, 
			      + DATA_TYPE, IS_GENERATED, GENERATION_EXPRESSION, IS_STORED, IS_HIDDEN, IS_UPDATABLE,
			      + IS_SYSTEM_DEFINED, IS_PARTITIONING_COLUMN, CLUSTERING_ORDINAL_POSITION, COLLATION_NAME,
			      + COLUMN_DEFAULT, ROUNDING_MODE 
				*/
				FieldAttributes tfa = new BigQueryHelper.FieldAttributes(
							BigQueryHelper.extractString(row, "TABLE_CATALOG"), 
							BigQueryHelper.extractString(row, "TABLE_SCHEMA"), 
							BigQueryHelper.extractString(row, "TABLE_NAME"), 
							BigQueryHelper.extractString(row, "COLUMN_NAME"), 
							BigQueryHelper.extractString(row, "ORDINAL_POSITION"), 
							BigQueryHelper.extractString(row, "IS_NULLABLE"), 
							BigQueryHelper.extractString(row, "DATA_TYPE"), 
							BigQueryHelper.extractString(row, "IS_GENERATED"), 
						    BigQueryHelper.extractString(row, "GENERATION_EXPRESSION"),
						    BigQueryHelper.extractString(row, "IS_STORED"), 
						    BigQueryHelper.extractString(row, "IS_HIDDEN"), 
						    BigQueryHelper.extractString(row, "IS_UPDATABLE"), 
						    BigQueryHelper.extractString(row, "IS_SYSTEM_DEFINED"), 
						    BigQueryHelper.extractString(row, "IS_PARTITIONING_COLUMN"), 
						    BigQueryHelper.extractString(row, "CLUSTERING_ORDINAL_POSITION"), 
						    BigQueryHelper.extractString(row, "COLLATION_NAME"), 
						    BigQueryHelper.extractString(row, "COLUMN_DEFAULT"), 
						    BigQueryHelper.extractString(row, "ROUNDING_MODE")
				);
				
				output.add(new BigQueryHelper.Field(tbl, tfa));
				
			}
			
			
		} catch(BigQueryException | JobException | InterruptedException  e) {
			
		}
		
		return output;
	}
	
	
	
	/***
	 * 
	 * @author ChadHarrison
	 *
	 */
	public static abstract class Object implements SafeNameable{
		public Object() {
			name = "";
		}
		
		public Object(String name) {
			this.name = name;
		}
		
		public static boolean equals(Object obj1, Object obj2) {
			boolean output = false;
			
			return output;
		}
		
		protected String name;
	}
	
	/***
	 * 
	 * @author ChadHarrison
	 *
	 */
	public static class Project extends Object{
		public Project() {
			super();
			id = "";
		}
		
		public Project(String projectID) {
			super();
			this.id = projectID;
		}
		
		public Project(String projectID, String projectName) {
			super(projectName);
			this.id = projectID;
		}
		
		public void setProjectName(String name) {
			this.name = name;
		}
		
		public String getProjectName() {
			return name;
		}
		
		public String getProjectID() {
			return id;
		}
		
		@Override
		public ObjectType getObjectType() {
			return ObjectType.PROJECT;
		}
				
		@Override
		public String getName() {
			return id;
		}

		@Override
		public String getSafeName() {
			return BigQueryHelper.getSafeName(id);
		}

		@Override
		public String getFullSafeName() {
			return getSafeName();
		}
		
		public static boolean equals(Project obj1, Project obj2) {
			boolean output = false;
			if(obj1.getFullSafeName().equalsIgnoreCase(obj2.getFullSafeName()))
				output = true;
			return output;
		}
		
		public boolean equals(Project obj2) {
			return equals(this, obj2);
		}
		
		private String id;
	}
	
	/***
	 * 
	 * @author ChadHarrison
	 *
	 */
	public static class Tag {
		Tag(){
			name = "";
		}
		
		public Tag(String name) {
			setName(name);
		}
		
		public void setName(String name) {
			this.name = name.toUpperCase();
		}
		
		public String getName() {
			return name;
		}
		
		private String name;
	}
	
	/***
	 * 
	 * @author ChadHarrison
	 *
	 */
	public static interface Taggable {
		public void setTagName(String name);
		public String getTagName();
	}
	
	/***
	 * 
	 * @author ChadHarrison
	 *
	 */
	public static class DataSet extends Object implements Taggable {
		DataSet() {
			super();
		}
		
		public DataSet(String name) {
			super(name);
		}
		
		public DataSet(Project project, String name) {
			super(name);
			this.Project = project;
		}
		
		public DataSet(Project project, String name, Tag tag) {
			super(name);
			Project = project;
			Tag = tag;
		}
		
		public String getProjectID() {
			return Project.getProjectID();
		}
		
		public String getProjectName() {
			return Project.getProjectName();
		}
		
		@Override
		public ObjectType getObjectType() {
			return ObjectType.DATASET;
		}
		
		@Override
		public String getName() {
			return name;
		}

		@Override
		public String getSafeName() {
			return BigQueryHelper.getSafeName(name);
		}
		
		@Override
		public String getFullSafeName() {
			if (Project.getFullSafeName() != "")
				return Project.getFullSafeName() + "." + BigQueryHelper.getSafeName(name);
			else
				return BigQueryHelper.getSafeName(name);
		}
		
		@Override
		public void setTagName(String name) {
			Tag.setName(name);
		}

		@Override
		public String getTagName() {
			return Tag.getName();
		}
		
		public static boolean equals(DataSet obj1, DataSet obj2) {
			boolean output = false;
			if(obj1.getFullSafeName().equalsIgnoreCase(obj2.getFullSafeName()))
				output = true;
			return output;
		}
		
		public boolean equals(DataSet obj2) {
			return equals(this, obj2);
		}
		
		Project Project = new Project();
		Tag Tag = new Tag();
	}

	/***
	 * 
	 * @author ChadHarrison
	 *
	 */
	public static class Alias {
		Alias() {
			name = "";
		}
		
		public Alias(String name) {
			setName(name);
		}
		
		public void setName(String name) {
			this.name = name;
		}
		
		public String getName() {
			return name;
		}
		
		public String getSafeName() {
			return BigQueryHelper.getSafeName(name);
		}
		
		private String name;
	}
	
	/***
	 * 
	 * @author ChadHarrison
	 *
	 */
	public static interface Aliasable {
		public void setAlias(String alias);
		public String getAlias();
		public String getFullSafeAlias();
	}
	
	/***
	 * 
	 * @author ChadHarrison
	 *
	 */
	public static class Table extends DataSet implements Aliasable {
		Table() {
			super();
		}
		
		public Table(String name) {
			super(name);
		}
		
		public Table(DataSet dataset, String name) {
			super(name);
			DataSet = dataset;
			Project = DataSet.Project;
		}
		
		public Table(DataSet dataset, String name, Alias alias) {
			super(name);
			DataSet = dataset;
			Alias = alias;
			Project = DataSet.Project;
		}
		
		public Table(DataSet dataset, String name, Tag tag) {
			super(name);
			DataSet = dataset;
			Tag = tag;
			Project = DataSet.Project;
		}
		
		public Table(DataSet dataset, String name, Alias alias, Tag tag) {
			super(name);
			DataSet = dataset;
			Alias = alias;
			Tag = tag;
			Project = DataSet.Project;
		}
		
		public String getDataSetName() {
			return DataSet.getName();
		}
		
		public String getSafeDataSetName() {
			return DataSet.getSafeName();
		}
		
		
		public String getFullSafeDataSetName() {
			return DataSet.getFullSafeName();
		}
		
		@Override
		public ObjectType getObjectType() {
			return ObjectType.TABLE;
		}
		
		@Override
		public String getFullSafeName() {
			if (DataSet.getFullSafeName() != "")
				return DataSet.getFullSafeName() + "." + BigQueryHelper.getSafeName(name);
			else
				return BigQueryHelper.getSafeName(name);
		}
		
		@Override
		public void setAlias(String name) {
			Alias.setName(name);
		}

		@Override
		public String getAlias() {
			return Alias.getName();
		}
		
		public String getSafeAlias() {
			if (Alias != null && !Alias.getName().equalsIgnoreCase(""))
				return Alias.getSafeName();
			else
				return this.getSafeName();
			
		}

		@Override
		public String getFullSafeAlias() {
			if(Alias.getName() != "" && name != "") 
				return getFullSafeName() + " " + Alias.getName();
			else if (name != "")
				return getFullSafeName();
			else return "";
		}
		
		public static boolean equals(Table obj1, Table obj2) {
			boolean output = false;
			if(obj1.getFullSafeName().equalsIgnoreCase(obj2.getFullSafeName()))
				output = true;
			return output;
		}
		
		public boolean equals(Table obj2) {
			return equals(this, obj2);
		}
		
		Alias Alias = new Alias();
		DataSet DataSet = new DataSet();
	}

	/***
	 * To capture field attributes shown in INFORMATION_SCHEMA.SCHEMATA
	 * 
	 * @author ChadHarrison
	 *
	 */
	public static class FieldAttributes {
		public FieldAttributes() {
			
		}

		public FieldAttributes(String tableCatalog, String tableSchema, String tableName, String columnName,
				String ordinalPosition, String isNullable, String dataType, String isGenerated,
				String generationExpression, String isStored, String isHidden, String isUpdateable,
				String isSystemDefined, String isPartitioningColumn, String clusteringOrdinalPosition,
				String collationName, String columnDefault, String roundingMode) {
			super();
			this.tableCatalog = tableCatalog;
			this.tableSchema = tableSchema;
			this.tableName = tableName;
			this.columnName = columnName;
			this.ordinalPosition = ordinalPosition;
			this.isNullable = isNullable;
			this.dataType = dataType;
			this.isGenerated = isGenerated;
			this.generationExpression = generationExpression;
			this.isStored = isStored;
			this.isHidden = isHidden;
			this.isUpdateable = isUpdateable;
			this.isSystemDefined = isSystemDefined;
			this.isPartitioningColumn = isPartitioningColumn;
			this.clusteringOrdinalPosition = clusteringOrdinalPosition;
			this.collationName = collationName;
			this.columnDefault = columnDefault;
			this.roundingMode = roundingMode;
		}

		public String getTableCatalog() {
			return tableCatalog;
		}

		public void setTableCatalog(String tableCatalog) {
			this.tableCatalog = tableCatalog;
		}

		public String getTableSchema() {
			return tableSchema;
		}

		public void setTableSchema(String tableSchema) {
			this.tableSchema = tableSchema;
		}

		public String getTableName() {
			return tableName;
		}

		public void setTableName(String tableName) {
			this.tableName = tableName;
		}

		public String getColumnName() {
			return columnName;
		}

		public void setColumnName(String columnName) {
			this.columnName = columnName;
		}

		public String getOrdinalPosition() {
			return ordinalPosition;
		}

		public void setOrdinalPosition(String ordinalPosition) {
			this.ordinalPosition = ordinalPosition;
		}

		public String getIsNullable() {
			return isNullable;
		}

		public void setIsNullable(String isNullable) {
			this.isNullable = isNullable;
		}

		public String getDataType() {
			return dataType;
		}

		public void setDataType(String dataType) {
			this.dataType = dataType;
		}

		public String getIsGenerated() {
			return isGenerated;
		}

		public void setIsGenerated(String isGenerated) {
			this.isGenerated = isGenerated;
		}

		public String getGenerationExpression() {
			return generationExpression;
		}

		public void setGenerationExpression(String generationExpression) {
			this.generationExpression = generationExpression;
		}

		public String getIsStored() {
			return isStored;
		}

		public void setIsStored(String isStored) {
			this.isStored = isStored;
		}

		public String getIsHidden() {
			return isHidden;
		}

		public void setIsHidden(String isHidden) {
			this.isHidden = isHidden;
		}

		public String getIsUpdateable() {
			return isUpdateable;
		}

		public void setIsUpdateable(String isUpdateable) {
			this.isUpdateable = isUpdateable;
		}

		public String getIsSystemDefined() {
			return isSystemDefined;
		}

		public void setIsSystemDefined(String isSystemDefined) {
			this.isSystemDefined = isSystemDefined;
		}

		public String getIsPartitioningColumn() {
			return isPartitioningColumn;
		}

		public void setIsPartitioningColumn(String isPartitioningColumn) {
			this.isPartitioningColumn = isPartitioningColumn;
		}

		public String getClusteringOrdinalPosition() {
			return clusteringOrdinalPosition;
		}

		public void setClusteringOrdinalPosition(String clusteringOrdinalPosition) {
			this.clusteringOrdinalPosition = clusteringOrdinalPosition;
		}

		public String getCollationName() {
			return collationName;
		}

		public void setCollationName(String collationName) {
			this.collationName = collationName;
		}

		public String getColumnDefault() {
			return columnDefault;
		}

		public void setColumnDefault(String columnDefault) {
			this.columnDefault = columnDefault;
		}

		public String getRoundingMode() {
			return roundingMode;
		}

		public void setRoundingMode(String roundingMode) {
			this.roundingMode = roundingMode;
		}

		private String tableCatalog = "";
		private String tableSchema = "";
		private String tableName = "";
		private String columnName = "";
		private String ordinalPosition = "";
		private String isNullable = "";
		private String dataType = "";
		private String isGenerated = "";
		private String generationExpression = "";
		private String isStored = "";
		private String isHidden = "";
		private String isUpdateable = "";
		private String isSystemDefined = "";
		private String isPartitioningColumn = "";
		private String clusteringOrdinalPosition = "";
		private String collationName = "";
		private String columnDefault = "";
		private String roundingMode = "";
	}
	
	/***
	 * 
	 * @author ChadHarrison
	 *
	 */
	public static class Field extends Table {
		
		//TODO: double check all constructors for desired results
		Field() {
			super();
		}
		
		public Field(String name) {
			super(name);
		}
		
		public Field(FieldAttributes fa) {
			super(fa.getColumnName());
			this.dataType = fa.getDataType();
			this.Attributes = fa;
		}
		
		public Field(Table table, String name) {
			super(name);
			Table = table;
			DataSet = Table.DataSet;
			Project = DataSet.Project;
		}
		
		public Field(Table table, String name, String datatype) {
			super(name);
			Table = table;
			DataSet = Table.DataSet;
			Project = DataSet.Project;
			this.dataType = datatype;
		}
		
		public Field(Table table, String name, Alias alias) {
			super(name);
			Table = table;
			Alias = alias;
			DataSet = Table.DataSet;
			Project = DataSet.Project;
		}
		
		public Field(Table table, String name, Tag tag) {
			super(name);
			Table = table;
			Tag = tag;
			DataSet = Table.DataSet;
			Project = DataSet.Project;
		}
		
		public Field(String name, Tag tag) {
			super(name);
			Tag = tag;
			DataSet = Table.DataSet;
			Project = DataSet.Project;
		}
		
		public Field(Table table, String name, Alias alias, Tag tag) {
			super(name);
			Table = table;
			Tag = tag;
			Alias = alias;
			DataSet = Table.DataSet;
			Project = DataSet.Project;
		}
		
		public Field(Table table, String name, String dataType, Alias alias, Tag tag) {
			super(name);
			this.dataType = dataType;
			Table = table;
			Tag = tag;
			Alias = alias;
			DataSet = Table.DataSet;
			Project = DataSet.Project;
		}
		
		public Field(Table table, String name, String dataType, Tag tag) {
			super(name);
			this.dataType = dataType;
			Table = table;
			Tag = tag;
			DataSet = Table.DataSet;
			Project = DataSet.Project;
		}
		
		
		public Field(Table table, String name, String dataType, Alias alias, Tag tag, Stack<Field> fieldStack) {
			super(name);
			this.dataType = dataType;
			Table = table;
			Tag = tag;
			Alias = alias;
			DataSet = Table.DataSet;
			Project = DataSet.Project;
			ParentFieldStack = fieldStack;
		}
		
		public Field(Table table, String name, String dataType, Alias alias, Stack<Field> fieldStack) {
			super(name);
			this.dataType = dataType;
			Table = table;
			Alias = alias;
			DataSet = Table.DataSet;
			Project = DataSet.Project;
			ParentFieldStack = fieldStack;
		}
		
		public Field(Table table, String name, String dataType, Stack<Field> fieldStack) {
			super(name);
			this.dataType = dataType;
			Table = table;
			DataSet = Table.DataSet;
			Project = DataSet.Project;
			ParentFieldStack = fieldStack;
		}
		
		public Field(Table table, FieldAttributes fa) {
			super(fa.getColumnName());
			dataType = fa.getDataType();
			Table = table;
			DataSet = Table.DataSet;
			Project = DataSet.Project;
		}
		
		public Field(Table table, FieldAttributes fa, Alias alias) {
			super(fa.getColumnName());
			dataType = fa.getDataType();
			Table = table;
			Alias = alias;
			DataSet = Table.DataSet;
			Project = DataSet.Project;
		}
		
		public Field(Table table, FieldAttributes fa, Tag tag) {
			super(fa.getColumnName());
			dataType = fa.getDataType();
			Table = table;
			Tag = tag;
			DataSet = Table.DataSet;
			Project = DataSet.Project;
		}
		
		public Field(Table table, FieldAttributes fa, Alias alias, Tag tag) {
			super(fa.getColumnName());
			dataType = fa.getDataType();
			Table = table;
			Tag = tag;
			Alias = alias;
			DataSet = Table.DataSet;
			Project = DataSet.Project;
		}
		
		public ArrayList<Field> expandFieldsFromDataType(){
			return expandNestedFieldsFromDataType(this);
		}
		
		/***
		 * Takes a datatype that contains embedded fields and expands to a set o
		 * Example: ARRAY<STRUCT<description STRING, domain STRING, url STRING>>
		 * @param fld
		 * @return
		 */

		
		/***
		 * Checks a String to see if its a BigQuery primitive data type.  Uses defined DataType 
		 * enum in this library to check.
		 * 
		 * 	ARRAY,
		 *	BIGNUMERIC,
		 *	BOOL,
		 *	BYTES,
		 *	DATE,
		 *	DATETIME,
		 *	FLOAT64,
		 *	GEOGRAPHY,
		 *	INT64,
		 *	INTERVAL,
		 *	JSON,
		 *	NUMERIC,
		 *	STRING,
		 *	STRUCT,
		 *	TIME,
		 *	TIMESTAMP
		 * @param datatype
		 * @return
		 */
		public static boolean isDataTypePrimitive(String datatype) {
			boolean output = false;
			for(BigQueryHelper.DataType dt : BigQueryHelper.DataType.values()) {
				//System.out.println(dt);
				if(dt.toString().equalsIgnoreCase(datatype)) {
					output = true;
					break;
				}
			}
			return output;
		}
		
		/***
		 * 
		 * @return
		 */
		public boolean isDataTypePrimitive() {
			return isDataTypePrimitive(this.dataType);
		}
		
		public String getTableName() {
			return Table.getName();
		}
		
		public String getSafeTableName() {
			return Table.getSafeName();
		}
		
		public String getFullSafeTableName() {
			return Table.getFullSafeName();
		}
		
		public String getDataType() {
			return dataType;
		}

		public void setDataType(String dataType) {
			this.dataType = dataType;
		}
		
		@Override
		public ObjectType getObjectType() {
			return ObjectType.FIELD;
		}
		
		@Override
		public String getFullSafeName() {
			if (Table.getFullSafeName() != "")
				return Table.getFullSafeName() + "." + BigQueryHelper.getSafeName(name);
			else
				return BigQueryHelper.getSafeName(name);
		}
		
		@Override
		public String getFullSafeAlias() {
			return getFullSafeAlias(true);
		}
		
		public String getFullSafeAlias(boolean suppressFieldAlias) {
			if(suppressFieldAlias == true) {
				if(Table.getSafeAlias() != "" && name != "")
					return Table.getSafeAlias() + "." + getSafeName();
				else if (name != "")
					return getSafeName();
				else return "";
			}
			else {
			
				if(Alias.getName() != "" && Table.getSafeAlias() != "" && name != "")
					return Table.getSafeAlias() + "." + getSafeName() + " " + Alias.getName();
				else if (Table.getSafeAlias() != "" && name != "")
					return Table.getSafeAlias() + "." + getSafeName();
				else if (name != "")
					return getSafeName();
				else return "";
			}
		}
		
		
		/***
		 * Returns the following based on the data type
		 * 	TEXT,NUMERIC,DATETIME,
		 * 
			2023-06-21 12:43:36.542: BOOL: 967
			2023-06-21 12:43:36.542: BYTES: 1
			2023-06-21 12:43:36.542: DATE: 130
			2023-06-21 12:43:36.543: DATETIME: 19
			2023-06-21 12:43:36.543: FLOAT64: 279
			2023-06-21 12:43:36.543: GEOGRAPHY: 2
			2023-06-21 12:43:36.543: INT64: 632
			2023-06-21 12:43:36.544: JSON: 2
			2023-06-21 12:43:36.544: NUMERIC: 20
			2023-06-21 12:43:36.544: STRING: 4645
			2023-06-21 12:43:36.544: TIME: 7
			2023-06-21 12:43:36.545: TIMESTAMP: 3106
			2023-06-21 12:43:36.545: 12 DataTypes
			2023-06-21 12:43:36.545: 1827809 Records

			
		 */
		public static String getAbstractDataType(String dataType) {
			String output = "";
				if(dataType.equalsIgnoreCase("STRING"))
					output = "TEXT";
				else
				if(dataType.equalsIgnoreCase("BOOL") ||
				   dataType.equalsIgnoreCase("NUMERIC") || 
				   dataType.equalsIgnoreCase("FLOAT64") || 
				   dataType.equalsIgnoreCase("INT64"))
					output = "NUMERIC";
				else
				if(dataType.equalsIgnoreCase("DATE") || 
				   dataType.equalsIgnoreCase("TIME") || 
				   dataType.equalsIgnoreCase("DATETIME") || 
				   dataType.equalsIgnoreCase("TIMESTAMP"))
					output = "DATETIME";
				else
				if(dataType.equalsIgnoreCase("JSON") || 
				   dataType.equalsIgnoreCase("GEOGRAPHY") || 
				   dataType.contains("ARRAY") || 
				   dataType.contains("STRUCT"))
					output = "COMPLEX";
				else output = "OTHER";
			return output;
		}
		
		
		public String getAbstractDataType() {
			return getAbstractDataType(this.getDataType());
		}
		/***
		 * Takes a complex datatype of (usually) nested fields and converts to set of columns
		 * with primitive datatypes.  It also converts datatypes to the following examples:
		 * 
		 * ARRAY<STRUCT<title STRING, url STRING>>
		 * 		-> title STRING
		 * 		-> url STRING
		 * 
		 * ARRAY<STRING> -> STRING
		 * STRING(500) -> STRING
		 * 
		 * TODO: factor code
		 * @param fld
		 * @return
		 */
		public static ArrayList<Field> expandNestedFieldsFromDataType(Field fld) {
			ArrayList<Field> output = new ArrayList<Field>();
			Stack<String> stack = new Stack<String>();
			
			TreeMap<String, Integer> cnts = new TreeMap<String, Integer>();
			
			int prevPos = 0;
			String curDelim = "";
			String prevDelim = "";
			String t = "";
			int i = 0;
			
			//QLog.log(fld.getFullSafeName() + " - " + fld.getDataType());
			
			//deals with complex nested datatypes.  Uses a stack to keep track of nesting level
			if(prevPos == 0 && fld.getDataType().contains("STRUCT")) {
				
				//if complex data type, go ahead and add base field name in stack
				//STRUCT is assumed
				stack.push(fld.getName() + " STRUCT");
						
				do {
					prevPos = i;
					i++;
					i = Field.getNextDelimPos(fld.getDataType(), i, ",|<|>");
					prevDelim = curDelim;
					curDelim = fld.getDataType().substring(i -1 , i);
					t = fld.getDataType().substring(prevPos, i - 1).trim();
					
					//QLog.log(prevDelim + curDelim + " - " + t + " " + i);
					
					if(curDelim.equalsIgnoreCase(",")) {
						if(!prevDelim.equalsIgnoreCase(">")) {
							//QLog.log(Field.stackToString(stack,t));
							output.add(new Field(fld.Table,t.split(" ")[0].trim(),t.split(" ")[1].trim(), 
									Field.cleanFieldStack(stack)));
						}
					} else 
					if(curDelim.equalsIgnoreCase("<")) {
						
						stack.push(t);
						if(t.split(" ").length > 1 && t.split(" ")[1].equalsIgnoreCase("ARRAY")) {
							String pt = t;
							prevPos = i;
							i++;
							i = Field.getNextDelimPos(fld.getDataType(), i, ",|<|>");
							prevDelim = curDelim;
							curDelim = fld.getDataType().substring(i - 1 , i);
							t = fld.getDataType().substring(prevPos, i -1).trim();
							
							//QLog.log(prevDelim + curDelim + " - " + t + " " + i + " new delim posture");
							
							//get field name in case like "FieldName STRING"
							t = pt.split(" ")[0] + " " + t.trim();
							
							//QLog.log("Addjusted to " + t);
							
							//STRUCTS usually indicate a new level nesting, push into stack
							if(t.split(" ")[1].equalsIgnoreCase("STRUCT"))
								stack.push(t);
							
							//checks new delimiter because we called next delimiter within this block of code
							if(curDelim.matches(">")) {
								//end of stack level, pop stack
								stack.pop();
								
								//avoids unintentional grabbing nothing fields with prevDelim + curDelim == ">>"
								if(prevDelim.matches(",|<")) {
									//QLog.log(Field.stackToString(stack,t));
									output.add(new Field(fld.Table,t.split(" ")[0].trim(),t.split(" ")[1].trim(), 
											Field.cleanFieldStack(stack)));
								}
							}
							if(curDelim.equalsIgnoreCase(",")) {
								//QLog.log(Field.stackToString(stack,t));
								output.add(new Field(fld.Table,t.split(" ")[0].trim(),t.split(" ")[1].trim(), 
										Field.cleanFieldStack(stack)));
							}
						}
						
						//QLog.log("Current Stack size: " + stack.size());
					} else 
					if(curDelim.equalsIgnoreCase(">")) {
						if(prevDelim.equalsIgnoreCase(",")) {
							//QLog.log(Field.stackToString(stack,t));
							output.add(new Field(fld.Table,t.split(" ")[0].trim(),t.split(" ")[1].trim(), 
									Field.cleanFieldStack(stack)));
						}
						
						//end of stack level, pop stack
						stack.pop();
					}
				

					if(!cnts.containsKey(prevDelim+curDelim))
						cnts.put(prevDelim+curDelim, 0);
					cnts.put(prevDelim + curDelim, cnts.get(prevDelim + curDelim) + 1);
					
				}
				while(i < fld.getDataType().length()) ;
			}				
			else {
				
				
				//QLog.log("checking " + fld.getDataType().replaceAll("[0-9]|[(]|[)]", ""));
				
				//Case:  STRING(500)
				if(Field.isDataTypePrimitive(fld.getDataType().replaceAll("[0-9]|[(]|[)]", ""))){
					//QLog.log("Field Length Specified: " +  fld.getName() + " to " + fld.getDataType().replaceAll("[0-9]|[(]|[)]",""));
					output.add(new Field(fld.Table, fld.getName(), fld.getDataType().replaceAll("[0-9]|[(]|[)]","").trim()));
				} else {
				//Case: ARRAY<STRING>
					String t1 = fld.getDataType().split("<|>")[0].trim();
					String t2 = fld.getDataType().split("<|>")[1].trim();
					if(t1.equalsIgnoreCase("ARRAY") && Field.isDataTypePrimitive(t2)) {
						//QLog.log("Array Field: " + fld.getName() + " " + t2);
						output.add(new Field(fld.Table, fld.getName(), t2.trim()));
					}
				}
				
				if(output.size() == 0) {
					QLog.log("Found unknown case: " + fld.getFullSafeName() + " " + fld.getDataType());
				}
					
			}
			/*
			QLog.log("get counts: ");
			for(Entry<String, Integer> entry : cnts.entrySet()) {
				QLog.log(entry.getKey() + " - " + entry.getValue());
			}
			
			QLog.log(i + " = i");
			*/
			return output;
		}
		
		/***
		 * Helper method for expandFieldsFromDataType
		 * @param fieldStack
		 * @return
		 */
		private static Stack<Field> cleanFieldStack(Stack<String> fieldStack) {
			Stack<Field> output = new Stack<Field>();
			
			for(int i = 0; i < fieldStack.size(); i++) {
				if(fieldStack.get(i).contains("STRUCT") && !fieldStack.get(i).equalsIgnoreCase("") && !fieldStack.get(i).equalsIgnoreCase("STRUCT")) {
					output.push(new Field(fieldStack.get(i).replaceFirst("STRUCT", "").trim()));
									
				}
			}
			
			return output;
		}
		
		/***
		 * Helper method for expandFieldsFromDataType
		 * @param stack
		 * @param cur
		 * @return
		 */
		private static String stackToString(Stack<String> stack, String cur) {
			String output = "";
			for(int i = 0; i < stack.size(); i++) {
				if(stack.get(i).contains("STRUCT") && !stack.get(i).equalsIgnoreCase("") && !stack.get(i).equalsIgnoreCase("STRUCT")) {
					
					output += BigQueryHelper.getSafeName(stack.get(i).replaceFirst("STRUCT", "").trim());
					output += ".";
					
				}
			}
			
			return output + cur;
		}
		
		/***
		 * Helper method for expandFieldsFromDataType
		 * @param stack
		 * @param cur
		 * @return
		 */
		private static int getNextDelimPos(String tstr, int curPos, String regexDelim) {
			int i = curPos;
			while(!tstr.substring(i - 1, i).matches(regexDelim) && i < tstr.length()) {
				//QLog.log(tstr.substring(i, i + 1));
				i++;
			}
			return i;
		}
		
		
		public static ArrayList<Field> filterByTable(ArrayList<Field> fldList, ArrayList<Table> tableFilter) {
			ArrayList<Field> output = new ArrayList<Field>();
			for(Field fld : fldList) {
				for(Table tbl: tableFilter) {
					if(tbl.equals(fld.Table)) 
						output.add(fld);
				}
			}
			return output;
		}
		
		public static boolean equals(Field obj1, Field obj2) {
			boolean output = false;
			if(obj1.getFullSafeName().equalsIgnoreCase(obj2.getFullSafeName()))
				output = true;
			return output;
		}
		
		public boolean equals(Field obj2) {
			return equals(this, obj2);
		}
		
		Table Table = new Table();
		FieldAttributes Attributes = new FieldAttributes();
		Stack<Field> ParentFieldStack = new Stack<Field>();
		
		private String dataType = "";


	}
	
	/***
	 * 
	 * @author ChadHarrison
	 *
	 */
	public static class TagLink {
		TagLink(Field fld1, Field fld2) {
			Field1 = fld1;
			Field2 = fld2;
		}
		
		public static boolean equals(TagLink tl1, TagLink tl2) {
			
			if(tl1.Field1.getFullSafeName().equalsIgnoreCase(tl2.Field1.getFullSafeName())  &&
			   tl1.Field2.getFullSafeName().equalsIgnoreCase(tl2.Field2.getFullSafeName())) {
				//System.out.println("case 1");
				return true;
			}
			else
			if(tl1.Field1.getFullSafeName().equalsIgnoreCase(tl2.Field2.getFullSafeName()) &&
			   tl1.Field2.getFullSafeName().equalsIgnoreCase(tl2.Field1.getFullSafeName())) {
				//System.out.println("case 2");
				return true;
			}
			else {
				//System.out.println("case 3");
				return false;
			}
		}
		
		public static boolean contains(ArrayList<TagLink> linkList, TagLink testLink) {
			boolean output = false;
			for(TagLink link : linkList) {
				if(equals(link, testLink)) {
					//System.out.println("found match");
					output = true;
					break;
				}
			}
			return output;
		}
		
		
		/***
		 * 
		 * TODO: Account for data types
		 * @param l1
		 * @param l2
		 * @return
		 */
		public static TreeMap<String, ArrayList<TagLink>> findTagMatches(ArrayList<Field> l1, ArrayList<Field> l2) {
			TreeMap<String, ArrayList<TagLink>> output = new TreeMap<String, ArrayList<TagLink>>();
			for(Field f1 : l1) {
				for(Field f2 : l2) {
	
					if(f1.getTagName().equalsIgnoreCase(f2.getTagName()) &&
					   f1.getDataType().equalsIgnoreCase(f2.getDataType())) {
						//make sure tag entry exists before attempt to add TagLink
						if(!output.containsKey(f1.getTagName()))
							output.put(f1.getTagName(), new ArrayList<TagLink>());
						
						output.get(f1.getTagName()).add(new TagLink(f1,f2));		
					}
				}
			}
			return output;
		}
		
		/***
		 * 
		 * @param l1
		 * @param l2
		 * @param tagName
		 * @return
		 */
		public static TreeMap<String, ArrayList<TagLink>> findTagMatches(ArrayList<Field> l1, ArrayList<Field> l2, TagGroup tagGroup) {
			TreeMap<String, ArrayList<TagLink>> output = new TreeMap<String, ArrayList<TagLink>>();
			for(Field f1 : l1) {
				for(Field f2 : l2) {
					for(String fieldTag : tagGroup.getAllUniqueLeafTagNames()) {
						if(f1.getTagName().equalsIgnoreCase(f2.getTagName()) &&
						   f1.getDataType().equalsIgnoreCase(f2.getDataType()) &&
						   f2.getTagName().equalsIgnoreCase(fieldTag)) {
							//make sure tag entry exists before attempt to add TagLink
							if(!output.containsKey(tagGroup.tagName.getName()))
								output.put(tagGroup.tagName.getName(), new ArrayList<TagLink>());
							
							output.get(tagGroup.tagName.getName()).add(new TagLink(f1,f2));		
						}
					}
				}
			}
			return output;
		}
		
		public static TreeMap<String, ArrayList<TagLink>> findTagMatches(ArrayList<Field> l1) {
			TreeMap<String, ArrayList<TagLink>> output = new TreeMap<String, ArrayList<TagLink>>();
			for(Field f1 : l1) {
				for(Field f2 : l1) {
					
					//make sure a table isn't linking to itself
					if(f1.getTagName().equalsIgnoreCase(f2.getTagName())  && 
					   f1.getDataType().equalsIgnoreCase(f2.getDataType()) &&
					   !f1.getFullSafeTableName().equalsIgnoreCase(f2.getFullSafeTableName())){
						//make sure tag entry exists before attempt to add TagLink
						
						if(!output.containsKey(f1.getTagName()))
							output.put(f1.getTagName(), new ArrayList<TagLink>());
						
						//make sure taglink doesn't already exist before adding
						TagLink tl = new TagLink(f1,f2);
						if(!TagLink.contains(output.get(f1.getTagName()), tl))
							output.get(f1.getTagName()).add(tl);		
					}
				}
			}
			return output;
		}
		
		public String getTagName() {
			if (Field1.getTagName().equalsIgnoreCase(Field2.getTagName())) {
				return Field1.getTagName();
			}
			else return "";
		}
		
		Field Field1;
		Field Field2;
	}
	
	/***
	 * 
	 * @author ChadHarrison
	 *
	 */
	public static class TagGroup {
		TagGroup(String name) {
			this.tagName = new Tag(name);
			tagList = new ArrayList<Tag>();
			tagGroups = new ArrayList<TagGroup>();
		}
		
		
		public int getLeafTagCount() {
			int output = 0;
			output = tagList.size();
			output = output + getLeafTagCount(tagGroups);
			return output;
		}	
		
		private int getLeafTagCount(ArrayList<TagGroup> tgs) {
			int output = 0;
			
			for(TagGroup tg : tgs) {
				if(tg.tagGroups.size() > 0)
					output = output + getLeafTagCount(tg.tagGroups);
				else output = output +  tg.getLeafTagCount();
			}
			
			return output;
		}
		

		/***
		 * Recursively finds all leaf nodes and returns ArrayList of Tags.
		 * Does not return unique list of tags.
		 * @return
		 */
		public ArrayList<Tag> getAllLeafTags() {
			ArrayList<Tag> output = new ArrayList<Tag>();
			if(this.tagList.size() > 0 )
				output.addAll(this.tagList);
			
			if(this.tagGroups.size() > 0)
				output.addAll(getAllLeafTags(this.tagGroups));
			
			return output;
		}
		
		private ArrayList<Tag> getAllLeafTags(ArrayList<TagGroup> tgs) {
			ArrayList<Tag> output = new ArrayList<Tag>();
			for(TagGroup tg : tgs) {
				if(tg.tagGroups.size() > 0)
					output.addAll(getAllLeafTags(tg.tagGroups)) ;
				else output.addAll(tg.tagList);
			}
			return output;
		}
		
		/***
		 * Recursively finds all unique leaf node tag names
		 * Use
		 * @return
		 */
		public ArrayList<String> getAllUniqueLeafTagNames() {
			ArrayList<String> output = new ArrayList<String>();
			for(Tag tag : this.getAllLeafTags()) {
				output.add(tag.getName());
			}
			
			//collapse list into unique list and then return
			return (ArrayList<String>) output.stream().distinct().collect(Collectors.toList());
		}

		public static boolean containsTagGroup(ArrayList<Field> flds, ArrayList<String> tagNames) {
			boolean output = false;
			
			for(String tagName : tagNames) {
				output = false;
				for(Field fld : flds) {
					if(fld.getTagName().equalsIgnoreCase(tagName)) {
						output = true;
						break;
					}
				}
				if(output == false)
					break;
			}
			
			return output;
		}
		
		/***
		 * Returns single suggested tag
		 * 
		 * @param fld
		 * @param tagGrps
		 * @return
		 */
		public static Tag suggestTag(Field fld, ArrayList<TagGroup> tagGrps) {
			Tag output = null;
			
			for(TagGroup tagGrp : tagGrps) {
				if(fld.getName().toUpperCase().contains(tagGrp.tagName.getName().toUpperCase())) {
					output = tagGrp.tagName;
				}
				else {
					for(String leafTagName : tagGrp.getAllUniqueLeafTagNames()) {
						if(fld.getName().toUpperCase().contains(leafTagName.toUpperCase())) {
							output = tagGrp.tagName;
							break;
						}
					}
				}
				
				if(output != null) 
					break;
			}
			return output;
		}

				
		Tag tagName;
		ArrayList<Tag> tagList;
		ArrayList<TagGroup> tagGroups;
	}
	

	/***
	 * Used to nest fields within a table object for hierarchical way of navigating fields
	 * 
	 * @author ChadHarrison
	 *
	 */
	public static class TableField extends Table {
		TableField() {
			super();
		}
		
		TableField(Table table){
			//super(DataSet dataset, String name, Alias alias, Tag tag) {
			super(table.DataSet, table.getName(), table.Alias, table.Tag);
		}
			
		/***
		 * 
		 * @param flds
		 * @return TreeMap<"TableName",TableField>
		 */
		public static TreeMap<String, TableField> convertFieldsToTableFieldTreeMap(ArrayList<Field> flds) {
			TreeMap<String, TableField> tbls = new TreeMap<String, TableField>();
			
			for(Field fld : flds) {
				if(!tbls.containsKey(fld.getFullSafeTableName()))
					tbls.put(fld.getFullSafeTableName(), new TableField(fld.Table));
				tbls.get(fld.getFullSafeTableName()).Fields.add(fld);
				
			}
			
			return tbls;
		}
		
		/***
		 * Converts an ArrayList of Fields into a linked list of tables that contains flds
		 * @param flds
		 * @return
		 */
		public static TableField convertFieldsToTableFieldLinkList(ArrayList<Field> flds) {
			TableField output = null;
			TreeMap<String, TableField> t = convertFieldsToTableFieldTreeMap(flds);
			
			for(Entry<String, TableField> entry : t.entrySet()) {
				
				if(output == null) {
					output = entry.getValue();
					output.firstTableField = entry.getValue();
				}
				else {
					output.setNext(entry.getValue());
					output = output.nextTableField;
				}
				
			}
			
			output = output.firstTableField;
			return output;
		}
		
		public static TreeMap<String, ArrayList<TagLink>> findTagMatches(ArrayList<Field> flds, ArrayList<TagGroup> tagGrps) {
			//1. get individual tagged fields
			TreeMap<String, ArrayList<TagLink>> output = TagLink.findTagMatches(flds);
			
			//2. get tag groups
			//note: using the linked list in this way reduces the total number of iterations
			TableField tfList = TableField.convertFieldsToTableFieldLinkList(flds);
			if(tfList.hasNext() && tfList.hasFirst()) {
				TableField list1 = tfList.firstTableField;
				
				while(list1 != null) {
					TableField list2 = list1.nextTableField;
					
					while(list2 != null) {
						
						for(TagGroup tg : tagGrps) {
							
							//make sure both tables contains the the group of tags before linking
							if(TagGroup.containsTagGroup(list1.Fields, tg.getAllUniqueLeafTagNames()) &&
							   TagGroup.containsTagGroup(list2.Fields, tg.getAllUniqueLeafTagNames()))
								output.putAll(TagLink.findTagMatches(list1.Fields, list2.Fields, tg));
						}

						list2 = list2.nextTableField;
					}
					
					list1 = list1.nextTableField;
				}
			}
			
			return output;
		}
		
		/*
		 * TODO: keep and finish or remove
		 * TODO
		 */
		public static TreeMap<String, ArrayList<TagLink>> findTagMatches(TableField l1) {
			TreeMap<String, ArrayList<TagLink>> output = new TreeMap<String, ArrayList<TagLink>>();
			
			if(l1.hasNext() && l1.hasFirst()) {
				TableField list1 = l1.firstTableField;
				
				while(list1 != null) {
					TableField list2 = list1.nextTableField;
					
					while(list2 != null) {
						System.out.println(list1.getFullSafeName() + " vs " + list2.getFullSafeName());
						list2 = list2.nextTableField;
					}
					
					list1 = list1.nextTableField;
				}
			}
			
			return output;
		}
		
		
		public boolean hasNext() {
			if(nextTableField != null)
				return true;
			else return false;
		}
		
		public boolean hasFirst() {
			if(firstTableField != null)
				return true;
			else return false;
		}
		
		public void setNext(TableField next) {
			if(this.firstTableField == null)
				this.firstTableField = this;

			next.firstTableField = this.firstTableField;
			this.nextTableField = next;

		}
		
		ArrayList<Field> Fields = new ArrayList<Field>();
		
		TableField nextTableField;
		TableField firstTableField;
	}
	
	
	/***
	 * 
	 * @author ChadHarrison
	 *
	 */
	public static class QueryBuilder {
		
		@FunctionalInterface
		public static interface Expression {
			String apply();
		}
		
		public static enum OutputType {
			PRINT,
			PRINTFILL,
			JOIN,
			SIMPLE
		}
		
		public static String toTrim(Field fld, OutputType tp) {
			String output = "TRIM("+ fill(fld, tp);
			return output;
		}
		
		public static String toTrim(Field fld, OutputType tp, Expression chainExpression) {
			String output = "";
			if(chainExpression != null)
				output = chainExpression.apply();
			output = "TRIM(" + output + fill(fld,tp) ;
			return output;
		}
		
		public static String toUpper(Field fld, OutputType tp) {
			String output = "UPPER("+ fill(fld, tp);
			return output;
		}
		
		public static String toUpper(Field fld, OutputType tp, Expression chainExpression) {
			String output = "";
			if(chainExpression != null)
				output = chainExpression.apply();
			output = "UPPER(" + output + fill(fld,tp) ;
			return output;
		}
		
		private static String fill(Field fld, OutputType tp) {
			String output = "";
			switch(tp) {
				case PRINT:
					output += fld.getFullSafeAlias(true) + ") " + fld.getSafeAlias();
					break;
				case PRINTFILL:
					output += ") " + fld.getSafeAlias();
					break;
				case JOIN:
					output += fld.getFullSafeAlias(true) + ")";
					break;
				case SIMPLE:
					output += ")";
					break;
				
				default:
			}
			return output;
		}
		
	}

}

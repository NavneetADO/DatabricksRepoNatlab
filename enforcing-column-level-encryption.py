# Databricks notebook source
# MAGIC %md #### Creating a Delta table

# COMMAND ----------

# DBTITLE 1,Creating a Delta table
# MAGIC %sql 
# MAGIC use default; -- Change this value to some other database if you do not want to use the Databricks default
# MAGIC
# MAGIC drop table if exists Test_Encryption;
# MAGIC
# MAGIC create table Test_Encryption(Name string, Address string, ssn string) USING DELTA;

# COMMAND ----------

# DBTITLE 1,Insert some test records
# MAGIC %sql
# MAGIC insert into Test_Encryption values ('John Doe', 'halloween street, pasadena', '3567812');
# MAGIC insert into Test_Encryption values ('Santa Claus', 'Christmas street, North Pole', '3568123');

# COMMAND ----------

# DBTITLE 1,Create a Fernet key and store it in secrets
# MAGIC %md #### Create a Fernet key and store it in secrets - https://cryptography.io/en/latest/fernet/
# MAGIC
# MAGIC ##### \*This step needs to be performed in a command line with Databricks CLI installed
# MAGIC
# MAGIC `pip install databricks-cli`  (Only needed if you do not have the Databricks CLI already installed)
# MAGIC
# MAGIC `pip install fernet`
# MAGIC
# MAGIC ##### Generate key using below code in Python
# MAGIC
# MAGIC `from cryptography.fernet import Fernet`
# MAGIC
# MAGIC `key = Fernet.generate_key()`
# MAGIC
# MAGIC ##### Once the key is generated, copy the key value and store it in Databricks secrets
# MAGIC
# MAGIC  `databricks secrets create-scope --scope encrypt`   <br>
# MAGIC  `databricks secrets put --scope encrypt --key fernetkey`
# MAGIC  
# MAGIC  Paste the key into the text editor, save, and close the program

# COMMAND ----------

# DBTITLE 1,Example to show how Fernet works
# Example code to show how Fernet works and encrypts a text string. DO NOT use the key generated below

from cryptography.fernet import Fernet
# >>> Put this somewhere safe!
key = Fernet.generate_key()
f = Fernet(key)
token = f.encrypt(b"A really secret message. Not for prying eyes.")
print(token)
print(f.decrypt(token))

# COMMAND ----------

# DBTITLE 1,Create Spark UDFs in python for encrypting a value and decrypting a value
# Define Encrypt User Defined Function 
def encrypt_val(clear_text,MASTER_KEY):
    from cryptography.fernet import Fernet
    f = Fernet(MASTER_KEY)
    clear_text_b=bytes(clear_text, 'utf-8')
    cipher_text = f.encrypt(clear_text_b)
    cipher_text = str(cipher_text.decode('ascii'))
    return cipher_text

# Define decrypt user defined function 
def decrypt_val(cipher_text,MASTER_KEY):
    from cryptography.fernet import Fernet
    f = Fernet(MASTER_KEY)
    clear_val=f.decrypt(cipher_text.encode()).decode()
    return clear_val
  

# COMMAND ----------

# DBTITLE 1,Use the UDF in a dataframe to encrypt a column
from pyspark.sql.functions import udf, lit, md5
from pyspark.sql.types import StringType

# Register UDF's
encrypt = udf(encrypt_val, StringType())
decrypt = udf(decrypt_val, StringType())

# Fetch key from secrets
encryptionKey = dbutils.preview.secret.get(scope = "encrypt", key = "fernetkey")

# Encrypt the data 
df = spark.table("Test_Encryption")
encrypted = df.withColumn("ssn", encrypt("ssn",lit(encryptionKey)))
display(encrypted)

#Save encrypted data 
encrypted.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("Test_Encryption_Table")

# COMMAND ----------

# DBTITLE 1,Decrypt a column in a dataframe
# This is how you can decrypt in a dataframe

decrypted = encrypted.withColumn("ssn", decrypt("ssn",lit(encryptionKey)))
display(decrypted)

# COMMAND ----------

# MAGIC %md #### Scala Code to create a custom hive UDF
# MAGIC
# MAGIC To use a spark UDF for creating a delta view it needs to be registered as permanent Hive UDF. This is the code to create the function. Create a jar with this scala function and upload it to dbfs as '/databricks/jars/decryptUDF.jar'.
# MAGIC
# MAGIC ```
# MAGIC import com.macasaet.fernet.{Key, StringValidator, Token}
# MAGIC import org.apache.hadoop.hive.ql.exec.UDF;
# MAGIC
# MAGIC class Validator extends StringValidator {
# MAGIC
# MAGIC   override def getTimeToLive() : java.time.temporal.TemporalAmount = {
# MAGIC     Duration.ofSeconds(Instant.MAX.getEpochSecond());
# MAGIC   }
# MAGIC }
# MAGIC
# MAGIC class udfDecrypt extends UDF {
# MAGIC
# MAGIC   def evaluate(inputVal: String, sparkKey : String): String = {
# MAGIC
# MAGIC     if( inputVal != null && inputVal!="" ) {
# MAGIC       val keys: Key = new Key(sparkKey)
# MAGIC       val token = Token.fromString(inputVal)
# MAGIC       val validator = new Validator() {}
# MAGIC       val payload = token.validateAndDecrypt(keys, validator)
# MAGIC       payload
# MAGIC     } else return inputVal
# MAGIC   }
# MAGIC }
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Create a Hive Function with the jar
# MAGIC %sql create function udfPIIDecrypt as 'com.nm.udf.udfDecrypt' using jar '/databricks/jars/decryptUDF.jar'

# COMMAND ----------

# DBTITLE 1,Save the secret in Spark Config
# MAGIC %md 
# MAGIC
# MAGIC ![ClusterImage](https://databricks.com/wp-content/uploads/2020/11/Save_secret.png)

# COMMAND ----------

# MAGIC %md #### Create Delta view using the function
# MAGIC We can pass secrets to the function by adding those secrets in spark configuration. 

# COMMAND ----------

# MAGIC %sql create view  Test_Encryption_PII as select name, address, udfPIIDecrypt(ssn, "${spark.fernet}") as ssn from Test_Encryption_Table

# COMMAND ----------

# MAGIC %sql select * from Test_Encryption_Table

# COMMAND ----------

# MAGIC %sql select * from Test_Encryption_PII

# COMMAND ----------

# MAGIC %md #### Key Rotation
# MAGIC
# MAGIC Use this command to rotate a key without the need to encrypting data again <br>
# MAGIC
# MAGIC newkey = f.rotate(fernetkey)
# MAGIC

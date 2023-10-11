OSA List Pipelines Example

Example class to show GGSA API for listing pipelines combined with Spark API output to get status of applications.
Written to be used with Oracle GoldenGate Stream Analytics OCI Marketplace version 19.1.0.0.6+
 
Execution:
java -jar ./target/osaListPipes-0.0.1-SNAPSHOT.jar [osaHost] [osaUser] [osaPassword] [sparkUser] [sparkPassword] [validateCertificate] [onlyActive] 

Parameters:
* osaHost - Hostname or IP of OSA server, for example 10.1.1.234
* osaUser - User for GGSA API , usually osaadmin
* osaPassword - Password to be used for given osaUser
* sparkUser - User for GGSA API , usually sparkadmin
* sparkPassword - Password to be used for given sparkUser
* validateCertificate - Use true if GGSA server has an SSL certificate signed by a CertificateAutority, false if default self-signed certificate is used.
* onlyActive - Use true if only active (running or previously run) pipelines should be listed, false if all pipelines should be listed
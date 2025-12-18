sftp -P 522 `
  -oHostKeyAlgorithms=+ssh-rsa `
  -oPubkeyAcceptedAlgorithms=+ssh-rsa `
  Harris_IUH@secure.edidrop.com

DBO6RS9pF18ouiW

sftp> cd 837P/OUT
sftp> get WCEDI_PAYOR_835_20251216104837_001.835 "J:/DATA DIMENSIONS/in/"
sftp> exit

sftp> cd 837I/OUT
sftp> get WCEDI_PAYOR_835_20251216104837_001.835 "J:/DATA DIMENSIONS/in/"
sftp> exit
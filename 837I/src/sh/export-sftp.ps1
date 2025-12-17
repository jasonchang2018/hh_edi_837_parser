sftp -P 522 `
  -oHostKeyAlgorithms=+ssh-rsa `
  -oPubkeyAcceptedAlgorithms=+ssh-rsa `
  Harris_IUH@secure.edidrop.com

DBO6RS9pF18ouiW

sftp> cd 837I/IN
sftp> put "J:\DATA DIMENSIONS\out\export-837I-HB-IUHEALTHTPL-20251118.837"
sftp> exit
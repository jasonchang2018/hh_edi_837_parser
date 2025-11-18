sftp -P 522 `
  -oHostKeyAlgorithms=+ssh-rsa `
  -oPubkeyAcceptedAlgorithms=+ssh-rsa `
  Harris_IUH@secure.edidrop.com

DBO6RS9pF18ouiW

sftp> cd 837P/IN
sftp> put C:\Users\jchang\Desktop\Projects\edi-837-parser\837P\data\out\export-837P-PB-IUHEALTHTPL-20251118.837
sftp> exit
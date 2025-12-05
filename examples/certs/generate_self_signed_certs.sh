#!/bin/bash

DOMAINS="barch barchie"

for x in $DOMAINS; do
cat <<EOF > /tmp/gen_cert.cnf
[req]
distinguished_name = req_distinguished_name
req_extensions = v3_req
default_md = sha256

[req_distinguished_name]
countryName = Country Name (2 letter code)
countryName_default = US
stateOrProvinceName = State or Province Name (full name)
stateOrProvinceName_default = Texas
localityName = Locality Name (eg, city)
localityName_default = San Antonio
organizationalUnitName  = Organizational Unit Name (eg, section)
organizationalUnitName_default  = Barch
commonName = Common Name (ie hostname or username)
commonName_default = $x
commonName_max  = 64

[ v3_req ]
# Extensions to add to a certificate request
basicConstraints = CA:FALSE
keyUsage = nonRepudiation, digitalSignature, keyEncipherment
EOF

openssl genrsa -out $x.key 4092
openssl req -new -batch -out $x.csr -key $x.key -config /tmp/gen_cert.cnf
openssl x509 -req -days 3650 -in $x.csr -signkey $x.key -out $x.pem
done
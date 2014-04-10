HBase backend design
=====================
Currently we are using two tables to serve as datastore for the crawler functions.

Frontier
---------------------
<b>Row-key</b>: reversed host Ex: com.apple.www
<b>Col-Family</b>: urls
<b>Col-Qualifier</b>: none

Ex:
                urls:
com.apple.com   /iPhone
                /iPad
                /support                                

Repository
---------------------
<b>Row-key</b>: reversed host-<hash> Ex: com.apple.www-ca0ae2e3a
<b>Col-Family</b>: urls
<b>Col-Qualifier-1</b>: url
<b>Col-Qualifier-2</b>: hash
<b>Col-Qualifier-3</b>: content


Ex:
                urls:
com.apple.com-ca0ae2e3ae   /iPhone   ca0ae2e3aefc56f4c071f475575915bc05074ba4   <html>...</html>
                           /iPad     91ecbb5330dfb106a6ee67a4c934f1305b50f40a   <html>...</html>
                           /support  eb6dce06aed2afc391b51acdd255976c818b8d97   <html>...</html>                                

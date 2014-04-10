HBase backend design
=====================
Currently we are using two tables to serve as datastore for the crawler functions.

Frontier
---------------------
<p><b>Row-key</b>: reversed host Ex: com.apple.www</p>
<p><b>Col-Family</b>: urls</p>
<p><b>Col-Qualifier</b>: none</p>

Ex:
                urls:
com.apple.com   /iPhone
                /iPad
                /support                                

Repository
---------------------
<p><b>Row-key</b>: reversed host-<hash> Ex: com.apple.www-ca0ae2e3a</p>
<p><b>Col-Family</b>: urls</p>
<p><b>Col-Qualifier-1</b>: url</p>
<p><b>Col-Qualifier-2</b>: hash</p>
<p><b>Col-Qualifier-3</b>: content</p>

<hr/>
Ex:
<table>
<tr>
  <td>com.apple.com-ca0ae2e3ae</td>
  <td>/iPhone</td>
  <td>ca0ae2e3aefc56f4c071f475575915bc05074ba4</td>
  <td>html content</td>
</tr>
<tr>
  <td>com.apple.com-ca0ae2e3ae</td>
  <td>/iPad</td>
  <td>91ecbb5330dfb106a6ee67a4c934f1305b50f40a</td>
  <td>html content</td>
</tr>
<tr>
  <td>com.apple.com-ca0ae2e3ae</td>
  <td>/support</td>
  <td>eb6dce06aed2afc391b51acdd255976c818b8d97</td>
  <td>html content</td>
</tr>
<tr>
  <td></td>
  <td></td>
  <td></td>
</tr>
</table>

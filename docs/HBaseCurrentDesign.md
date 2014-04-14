# HBase backend design
Currently we are using two tables to serve as datastore for the crawler functions.

## Frontier

+ **Row-key**: reversed host <br/>
>`com.apple.www`

+ **Col-Family**: urls<br/>

<table>
  <tr>
    <th>row</th>
    <th>urls:</th>
  </tr>
  <tr>
    <td rowspan="3">`com.apple.www`</td>
    <td>/iPhone</td>
  </tr>
  <tr>
    <td>/iPad</td>
  </tr>
  <tr>
    <td>/support</td>
  </tr>
</table>

## Repository
+ **Row-key**: reversed host-*hash* <br/>
>`com.apple.www-ca0ae2e3a`
+ **Col-Family**: urls<br/>
    + **Col-Qualifier-1**: url<br/>
    + **Col-Qualifier-2**: hash<br/>      
    + **Col-Qualifier-3**: content<br/>

An Example of the structure of repository:

<table>
<tr>
  <td>com.apple.com-ca0ae2e3ae</td>
  <td>/iPhone</td>
  <td>ca0ae2e3aefc56f4c071f475575915bc05074ba4</td>
  <td>html content</td>
</tr>
<tr>
  <td>com.apple.com-91ecbb5330</td>
  <td>/iPad</td>
  <td>91ecbb5330dfb106a6ee67a4c934f1305b50f40a</td>
  <td>html content</td>
</tr>
<tr>
  <td>com.apple.com-eb6dce06ae</td>
  <td>/support</td>
  <td>eb6dce06aed2afc391b51acdd255976c818b8d97</td>
  <td>html content</td>
</tr>
</table>
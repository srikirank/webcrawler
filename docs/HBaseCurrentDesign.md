# HBase backend design
Currently we are using two tables to serve as datastore for the crawler functions.

## Frontier

+ **Row-key**: hash <br/>
>`ca0ae2e3aefc56f4c071f475575915bc05074ba4`

+ **Col-Family**: urls<br/>
    + **Qualifier** : address<br/>

<table>
  <tr>
    <th>row</th>
    <th>urls:address</th>
  </tr>
  <tr>
    <td>91ecbb5330dfb106a6ee67a4c934f1305b50f40a</td>
    <td>com.apple.www/iPad</td>
  </tr>  
  <tr>
    <td>ca0ae2e3aefc56f4c071f475575915bc05074ba4</td>
    <td>com.apple.www/iPhone</td>
  </tr>
  <tr>
    <td>eb6dce06aed2afc391b51acdd255976c818b8d97</td>
    <td>com.apple.www/support</td>
  </tr>
</table>

## Repository
+ **Row-key**: reversed host-*hash* <br/>
>`com.apple.www-ca0ae2e3aefc56f4c071f475575915bc05074ba4`

+ **Col-Family**: urls<br/>
    + **Col-Qualifier-1**: url<br/>
    + **Col-Qualifier-2**: hash<br/>
    + **Col-Qualifier-3**: content<br/>

An Example of the structure of repository:

<table>
<tr>
  <th>row</th>
  <th>urls:url</th>
  <th>urls:content</th>
</tr>
<tr>
  <td>com.apple.com-91ecbb5330dfb106a6ee67a4c934f1305b50f40a</td>
  <td>/iPhone</td>
  <td>html content</td>
</tr>
<tr>
  <td>com.apple.com-ca0ae2e3aefc56f4c071f475575915bc05074ba4</td>
  <td>/iPad</td>
  <td>html content</td>
</tr>
<tr>
  <td>com.apple.com-eb6dce06aed2afc391b51acdd255976c818b8d97</td>
  <td>/support</td>
  <td>html content</td>
</tr>
</table>
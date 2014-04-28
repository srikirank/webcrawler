# HBase backend design
Currently we are using two tables to serve as datastore for the crawler functions.

## Crawled

+ **Row-key**: Reversed Domain <br/>
>`com.url1`

+ **Col-Family**: urls<br/>
    + **Qualifier** : address<br/>

<table>
  <tr>
    <th>row</th>
    <th>urls:ca0ae2e3aefc56f</th>
    <th>urls:eb6dce06aed2afc</th>
  </tr>
  <tr>
    <td>com.url1</td>
    <td>www.url1.com/xyz1</td>
    <td>www.url1.com/xyz2</td>
  </tr>  
  <tr>
    <th></th>
    <th>urls:ca0ae2e3aefc56f4c</th>
    <th>urls:eb6dce06aed2afc39</th>
  </tr>
  <tr>
    <td>com.url1</td>
    <td>www.url1.com/xyz1</td>
    <td>www.url1.com/xyz2</td>
  </tr>    
</table>

## Repository
+ **Row-key**: reversed host-*hash* <br/>
>`com.apple.www-ca0ae2e3aefc56f`

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
  <th>outgoing:links</th>
</tr>
<tr>
  <td>com.apple.com-91ecbb5330dfb1</td>
  <td>/xyz2</td>
  <td>html content</td>
  <td>f4c071f4755759,91ecbb5330d</td>
</tr>
<tr>
  <td>com.apple.com-ca0ae2e3aefc56f4c071f475575915bc05074ba4</td>
  <td>/xyz1</td>
  <td>html content</td>
  <td>f4c071f4755759,91ecbb5330d</td>
</tr>
</table>
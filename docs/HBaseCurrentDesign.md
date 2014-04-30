# HBase backend design
Currently we are using two tables to serve as datastore for the crawler functions.

## Crawled

+ **Row-key**: Reversed Domain <br/>
>`com.apple`

+ **Col-Family**: urls<br/>
    + **Qualifier** : address<br/>

<table>
  <tr>
    <th>row</th>
    <th>urls:ca0ae2e3aefc56f</th>
    <th>urls:eb6dce06aed2afc</th>
  </tr>
  <tr>
    <td>com.apple</td>
    <td>www.apple.com/about</td>
    <td>www.apple.com/products</td>
  </tr>  
  <tr>
    <th></th>
    <th>urls:ca0ae2e3aefc56f4c</th>
    <th>urls:eb6dce06aed2afc39</th>
  </tr>
  <tr>
    <td>org.wikipedia</td>
    <td>en.wikipedia.org/wiki/Java</td>
    <td>en.wikipedia.org/wiki/XML/</td>
  </tr>    
</table>

## Repository
+ **Row-key**: reversed host-*hash* <br/>
>`com.apple.www-ca0ae2e3aefc56f`

+ **Col-Family**: urls<br/>
    + **Col-Qualifier-1**: url<br/>
    + **Col-Qualifier-2**: hash<br/>
    
+ **Col-Family**: content<br/>
    + **Col-Qualifier**: body<br/>

+ **Col-Family**: outgoing<br/>
    + **Col-Qualifier**: links<br/>

An Example of the structure of repository:

<table>
<tr>
  <th>row</th>
  <th>urls:url</th>
  <th>urls:hash</th>
  <th>content:body</th>
  <th>outgoing:links</th>
</tr>
<tr>
  <td>com.apple.com-91ecbb5330dfb1</td>
  <td>/about</td>
  <td>91ecbb5330dfb1</td>
  <td>html content</td>
  <td>hgc071f4755759,91ecbb5330d</td>
</tr>
<tr>
  <td>com.apple.com-ca0ae2e3aefc56f</td>
  <td>/products</td>
  <td>ca0ae2e3aefc56f</td>
  <td>html content</td>
  <td>f4c071f4685759,10ecbb5330d</td>
</tr>
</table>

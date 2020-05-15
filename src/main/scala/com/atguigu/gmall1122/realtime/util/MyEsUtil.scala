package com.atguigu.gmall1122.realtime.util

import java.util

import io.searchbox.client.config.HttpClientConfig
import io.searchbox.client.{JestClient, JestClientFactory}
import io.searchbox.core.{Bulk, BulkResult, Index, Search, SearchResult}
import org.elasticsearch.index.query.MatchQueryBuilder
import org.elasticsearch.search.builder.SearchSourceBuilder
import org.elasticsearch.search.sort.SortOrder

import scala.collection.mutable.ListBuffer

object MyEsUtil {


  private    var  factory:  JestClientFactory=null;

  def getClient:JestClient ={
    if(factory==null)build();
    factory.getObject

  }

  def  build(): Unit ={
    factory=new JestClientFactory
    factory.setHttpClientConfig(new HttpClientConfig.Builder("http://hadoop1:9200" )
      .multiThreaded(true)
      .maxTotalConnection(20)
      .connTimeout(10000).readTimeout(10000).build())
  }


  // batch
  def saveBulk(dataList:List[(String,AnyRef)],indexName:String ): Unit ={
    if(dataList!=null&&dataList.size>0){
      val jest: JestClient = getClient
      val bulkBuilder = new Bulk.Builder()
      bulkBuilder.defaultIndex(indexName).defaultType("_doc")
      for ((id,data) <- dataList ) {
        val index: Index = new Index.Builder(data).id(id).build()
        bulkBuilder.addAction(index)
      }

      val bulk: Bulk = bulkBuilder.build()
      val items: util.List[BulkResult#BulkResultItem] = jest.execute(bulk).getItems
      println("已保存："+items.size()+"条数据！")
      jest.close()
    }
  }




  def main(args: Array[String]): Unit = {
        val jest: JestClient = getClient
       // any ==>  case class
        //val index =  new Index.Builder(Movie(4,"红海战役",9.0)).index("movie_chn1122").`type`("movie").id("4").build()
        val query="{\n  \"query\": {\n    \"match\": {\n      \"name\": \"红海战役\"\n    }\n  }\n}"
        val sourceBuilder = new SearchSourceBuilder
         sourceBuilder.query(new MatchQueryBuilder("name","红海战役"))
         sourceBuilder.sort("doubanScore",SortOrder.ASC)
        val query2: String = sourceBuilder.toString
        println(query2)
        val search = new Search.Builder(query2).addIndex("movie_chn1122").addType("movie").build()
        val result: SearchResult = jest.execute(search )
         val movieRsList: util.List[SearchResult#Hit[Movie, Void]] = result.getHits(classOf[Movie])
         import scala.collection.JavaConversions._
        val movieList=ListBuffer[Movie]()
        for (hit <- movieRsList ) {
          val movie: Movie = hit.source
          movieList+= movie
        }
        println(movieList.mkString("\n"))
        jest.close()
  }


  case class Movie(id:Long,name:String,doubanScore:Double){


  }


}

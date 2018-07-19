package fr.htc.sofspak;


import org.apache.hadoop.fs.DF;
import org.apache.hadoop.util.hash.Hash;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.ReduceFunction;
import org.apache.spark.sql.DataFrameNaFunctions;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.parser.SqlBaseParser.FromClauseContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import scala.Function1;
import scala.Tuple2;
import scala.runtime.BoxedUnit;

import static org.apache.spark.sql.functions.col;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

public class SparkDataSetFram {

	public static boolean exist(HashMap<Integer,String> Mapo, String Tr) {
		
		 if (Mapo.containsValue(Tr)) return true;
		 else return false;
	}

	
	private static final Pattern SPACE = Pattern.compile(" ");

	public static Pattern getSpace() {
		return SPACE;
	}

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("Spark work").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

		
       
	SparkSession spark = SparkSession.builder()
			    .config("spark.sql.warehouse.dir", "file:///C:/Users/dev")
			    .appName(" basic example")
			    .master("local")
		        .getOrCreate();
            /*String lines = sc.textFile("C:\\Users\\Desktop\\mapred.txt");
            JavaRDD<String> RDD1 = sc.textFile(lines);
			 JavaRDD<String> RDD1 = lines.flatMap("_".split("\\s+"));
			  val wc = words.map(w => (w, 1)).reduceByKey(_ + _)
			  wc.saveAsTextFile("file1.count")val lines = sc.textFile("file1.txt")
					  val words = lines.flatMap(_.split("\\s+"))
					  val wc = words.map(w => (w, 1)).reduceByKey(_ + _)
					  wc.saveAsTextFile("file1.count");*/
			
	         JavaRDD<String> RDD1 = sc.textFile(lines);
	        JavaRDD<String> flatmap = RDD1.flatMap(new FlatMapFunction<String, String>() {

				@Override
				public Iterator<String> call(String t) throws Exception {
					String []tablo = t.split(";");
					ArrayList<String> maliste = new ArrayList<String>();
					for(String str :tablo) {
						maliste.add(str);
				}
					return maliste.iterator();
				}
			});
	       //System.out.println(flatmap.first());
	       

	       JavaPairRDD<String,Integer> mapairrd =flatmap.mapToPair(new PairFunction<String, String,Integer>() {
	    	

			@Override
				public Tuple2<String, Integer> call(String line) throws Exception {
				int i=0;
					String[] tabline = line.split(" ");
					String var = tabline [i];
					
					
					/*int cle=0;
					String[] tabline1 = new String[tabline.length];
					int[] cles = new int[tabline.length];
					boolean ch = true;
					
					
					for(int i =0; i< tabline.length; i++) {
						
						int cond = 0;
						String mot = tabline[i];
						existe = -1;
						ch = true;
						
						for (int h=i; h< tabline1.length; h++) {
							if (tabline[i] == tabline1[h] && ch != false)
							{
								existe = h;
								ch = false;
							}
						}
						
						
						if (existe == -1) {
							tabline1[i] = tabline[i];
							cles[i] = i;
						}
						if(existe != -1 ) {
							cles[i] = existe;

						}
						
					}
					
					for (int i=0; i<cles.length; i++) {
						System.out.println(" "+tabline1[i]+"  clés: "+cles[i]);
					}*/
					
						

					
					Tuple2<String,Integer> mytuple = new Tuple2<String,Integer>(var,1);
					{
					
					}				
					 

			return mytuple;
			
				}
				
				
			});
 System.out.println(mapairrd.first());
         JavaPairRDD<String, Integer> myreducer = mapairrd.reduceByKey(new Function2<Integer, Integer, Integer>() {
			
			@Override
			public Integer call(Integer v1, Integer v2) throws Exception {
				
				return (v1+v2);
			}
		});
         System.out.println(myreducer.collect());
         
	        
	

	//JavaRDD<String> maRDD = sc.textFile(SalPath);
	
	
	            
	    		
        
			
	     
	// Crée un RDD 
	
	JavaRDD < String > peopleRDD  = sc.textFile(filepath);
			  

			// Le schéma est codé dans une chaîne 
			String  schemaString  = "adresseIP colo nield Date Zone methode getRessources protocoleHTTP Requette Erreur memoryconsum siteDeConnexion AgentReqHTTP OSVersionclient" ;
			// Génère le schéma en fonction de la chaîne de schéma 
			// Convertir les enregistrements du RDD en Lignes (commes une ligne de colonne)
			List<StructField> fields = new ArrayList<>();
			for (String fieldName : schemaString.split(" ")) {
			  StructField field = DataTypes.createStructField(fieldName, DataTypes.StringType, true);
			  fields.add(field);
			}
			StructType schema = DataTypes.createStructType(fields);
			JavaRDD<Row> rowRDD = peopleRDD.map((Function<String, Row>) record -> {
				  String[] attributes = record.split(" ");
				  return RowFactory.create(attributes[0],attributes[1],attributes[2],attributes[3],attributes[4],attributes[5],attributes[6],attributes[7],attributes[8],attributes[9],attributes[10],attributes[11],attributes[12],attributes[13].trim());
				});
			// // Appliquer le schéma à la RDD 
			Dataset<Row> peopleDataFrame = spark.createDataFrame(rowRDD, schema);
			//peopleDataFrame.printSchema();
			// Crée une vue temporaire en utilisant le DataFrame 
			peopleDataFrame.createOrReplaceTempView("people");
			// SQL peut être exécuté sur une vue temporaire créée en utilisant DataFrames 
			Dataset<Row> results = spark.sql("SELECT methode from people where methode LIKE '%G%'");
			results.cache();
			results.show();
		   JavaRDD<Row> mycount =results.toJavaRDD();
		  
			
		
				
		
			
		Dataset<Row> dataSetfortext = spark.read()
	    		   .format("csv")
	    		   .option("header","true")
	    		   .option("separator"," ")
	    		   .option("inferSchema","true")
	    		   .load(path);
		//dataSetfortext.printSchema();
		dataSetfortext.show();
		//dataSetforjson.createOrReplaceTempView("amfjs");
		//Dataset<Row> sql2 = spark.sql("select * from amfjs");
		//sql2.printSchema();
		//Dataset<Row> mydfcsv=  sql2.
		
		
			   
	   
      /* Dataset<Row> dataSetAmf = spark.read()
    		   .format("csv")
    		   .option("header","true")
    		   .option("separator",",")
    		   .option("inferSchema","true")
    		   .load(filePath);*/
      // dataSetAmf.createOrReplaceTempView("peaple");
       //Dataset<Row> sqlreq = spark.sql("select Types_Operations,substr(Date_du_document,0,4) As Annee_Operation From peaple " );
      //sqlreq.show(50);
     // sqlreq.write().csv("C:/Users/Sofiane/Desktop/bigdatayasm/sofianeHD/monfich.csv");
      //JavaRDD<Row> myRdd = sqlreq.toJavaRDD();
     
     
       
}
	}





package org.allofus.curation.pipeline;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.Element;
import org.apache.beam.sdk.transforms.DoFn.OutputReceiver;
import org.apache.beam.sdk.transforms.DoFn.ProcessElement;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.csv.CSVRecord;
import org.apache.uima.UIMAException;
import org.apache.uima.analysis_engine.AnalysisEngineProcessException;
import org.apache.uima.resource.ResourceInitializationException;
import org.apache.uima.util.InvalidXMLException;
import org.xml.sax.SAXException;

import edu.uth.clamp.config.ConfigUtil;
import edu.uth.clamp.config.ConfigurationException;
import edu.uth.clamp.io.DocumentIOException;
import edu.uth.clamp.nlp.encoding.MaxMatchingUmlsEncoderCovid;
import edu.uth.clamp.nlp.encoding.RxNormEncoderUIMA;
import edu.uth.clamp.nlp.main.ClampLauncher;
import edu.uth.clamp.nlp.structure.ClampNameEntity;
import edu.uth.clamp.nlp.structure.ClampRelation;
import edu.uth.clamp.nlp.structure.ClampSentence;
import edu.uth.clamp.nlp.structure.ClampToken;
import edu.uth.clamp.nlp.structure.DocProcessor;
import edu.uth.clamp.nlp.uima.UmlsEncoderUIMA;
import edu.uth.clamp.nlp.structure.Document;


public class RunCLAMPStringFn extends DoFn<CSVRecord, String>  {
//public class RunCLAMPFn extends DoFn<String, String>  {
	static String inText;
	static String inExt;
	static List<DocProcessor> procList = new ArrayList<DocProcessor>();
	static List<File> inputList = new ArrayList<File>();
	static File outPath = null;
	static File umlsIndex = null;
		
	public static int init_clamp( CurationNLPOptions options ) {
		Instant start = Instant.now();
		String inExt = options.getInputExt();
		String inText = options.getInput();	
		String outDir = options.getOutput();	
		String pipeline_file = options.getPipeline();	
		String umlsIndexDir = options.getUmls_index();
        System.out.println(inText);
        System.out.println(inExt);
        System.out.println(outDir);
        System.out.println(pipeline_file);
        System.out.println(umlsIndexDir);
		// check output dir;
		outPath = new File( outDir );
		if( !outPath.exists() ) {
			System.out.println( "output folder doesn't exist. outputDir=[" + outDir + "]" );
			return -1;
		} else if( !outPath.isDirectory() ) {
			System.out.println( "output should be a folder. outputDir=[" + outDir + "]" );
			return -1;
		}
    	
	    umlsIndex = new File( umlsIndexDir );
		if( !umlsIndex.exists() ) {
				System.out.println( "cannot find umls index directory. index=[" + umlsIndexDir + "]" );
				return -1;
		} else if( !umlsIndex.isDirectory() ) {
			System.out.println( "umls index should be a directory. index=[" + umlsIndexDir + "]" );
			return -1;
		}
		
		// pipeline
		File pipelineJar = new File( pipeline_file );
		if( !pipelineJar.exists() ) {
			System.out.println( "pipeline jar doesn't exist. pipelineJar=[" + pipeline_file + "]" );
			return -1;
		} else if( !pipelineJar.isFile() ) {
			System.out.println( "pipeline jar should be a jar file or a .pipefile. pipelineJar=[" + pipeline_file + "]" );
			return -1;
		}
		List<DocProcessor> pipeline = null;
		try {
			// load pipelines;
			if( pipelineJar.getName().endsWith( ".pipeline" ) ) {
				pipeline = ConfigUtil.importPipelineFromFile( pipelineJar );
			} else if( pipelineJar.getName().endsWith( ".jar" ) ) {
				pipeline = ConfigUtil.importPipelineFromJar( pipelineJar );
			} else {
				System.out.println( "pipeline jar should be a jar file or a .pipefile. pipelineJar=[" + pipeline_file + "]" );
				return -1;
			}
		
			for( DocProcessor proc :  pipeline ) {
				if( proc instanceof UmlsEncoderUIMA ) {
					((UmlsEncoderUIMA)proc).setIndexDir( umlsIndex );
					procList.add( proc );
				} else if( proc instanceof RxNormEncoderUIMA ) {
					File index = new File( umlsIndex.getParent() + "/rxnorm_index/" );
					((RxNormEncoderUIMA)proc).setIndex( index.getAbsolutePath() );
					procList.add( proc );
				} else if( proc instanceof MaxMatchingUmlsEncoderCovid ) {
					File index = new File( umlsIndex.getParent() + "/umls_index_lucene8_covid/" );
					((MaxMatchingUmlsEncoderCovid)proc).setIndexDir( index.getAbsolutePath() );
					procList.add( proc );
				} else {
					procList.add( proc );
				}				
			}
		} catch (ConfigurationException e) {
			System.err.println("Invalid configuration file in the pipeline export");			
			System.err.println("The file seems to be corrupted.");			
			System.err.println(e.getMessage());
			e.printStackTrace();
			return -1;
		} catch (IOException e) {
			//log.throwing(ClampLauncher.class.getName(), "parseArgs", e);
			System.err.println("Error occured during opening the pipeline.");			
			System.err.println(e.getMessage());
			e.printStackTrace();
			return -1;
		} catch (DocumentIOException e) {
			//log.throwing(ClampLauncher.class.getName(), "parseArgs", e);
			System.err.println("Error occured paring the pipeline.");			
			System.err.println(e.getMessage());
			e.printStackTrace();
			return -1;
		}
		
    	Instant end = Instant.now();
    	Duration timeElapsed = Duration.between(start, end);
    	System.out.println("init CLAMP: Time taken: "+ timeElapsed.toMillis() +" milliseconds");    	
		
		return 0;
	}
	
    @ProcessElement
    //public void processElement(@Element String element, OutputReceiver<String> receiver) {
    public void processElement(@Element CSVRecord csvRecord, OutputReceiver<String> receiver) throws DocumentIOException {
    	try {
    		    //String filename = file.getMetadata().resourceId().getFilename().toString();
    		    String fileid = csvRecord.get(0); // col[0] for fileid/filename
    		    String text = csvRecord.get(1);  // col[1] for text
		        //String fileid = "all-of-us-test";
		        //String text = "just for test file text";    		
    			Document doc = new Document(fileid, text); // = new Document( inPath.getAbsolutePath() + File.separator + filename );
    		    //Document doc = new Document("test.txt"); // = new Document( inPath.getAbsolutePath() + File.separator + filename );
    			for( DocProcessor proc : procList ) {
    				try {
    					proc.process( doc );
    				} catch (AnalysisEngineProcessException e) {
    					// TODO Auto-generated catch block
    					e.printStackTrace();
    				}
    			}
    			// java File object
    			//doc.save( outPath.getAbsolutePath() + File.separator + filename.replace( ".txt", ".xmi" ) );

    			FileWriter outfile = new FileWriter( new File( outPath.getAbsolutePath() + File.separator + fileid ) );
    			for( ClampNameEntity cne : doc.getNameEntity() ) {
    				if( cne.getUmlsCui() != null && !cne.getUmlsCui().isEmpty() ) {
    					receiver.output( "\tcui=" + cne.getUmlsCui() );
    					outfile.write("\tcui=" + cne.getUmlsCui());
    				}
    				receiver.output( "\tne=" + cne.textStr().replace( "\r\n", " " ).replace( "\n", " ") );
    				outfile.write("\tne=" + cne.textStr().replace( "\r\n", " " ).replace( "\n", " "));
    				outfile.write( "\n" );
    			}


    			outfile.close();
    	} catch (IOException | UIMAException e) {
    		// TODO Auto-generated catch block
    		e.printStackTrace();
    	}
        System.out.println(receiver);
    	System.out.println( "document processed.." );
    }
		
    
    public static void main( String[] argv ) throws InvalidXMLException, ResourceInitializationException, ParseException, org.apache.commons.cli.ParseException {
             // -i %input% -o %output% -p %pipeline% -A %umlsAPI% -I  %umlsIndex%
        CurationNLPOptions options = 
                PipelineOptionsFactory.fromArgs(argv).withValidation().as(CurationNLPOptions.class);
    	
		init_clamp(options);
        //run_pipeline();
        System.out.println("Run CLAMP pipeline done.");
    }
}

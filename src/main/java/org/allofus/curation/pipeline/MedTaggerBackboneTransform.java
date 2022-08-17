

package org.allofus.curation.pipeline;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
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
import edu.uth.clamp.nlp.uima.DocProcessor;
import edu.uth.clamp.nlp.uima.UmlsEncoderUIMA;
import edu.uth.clamp.nlp.structure.Document;


public class RunCLAMPPipeline {
	static List<DocProcessor> procList = new ArrayList<DocProcessor>();
	static List<File> inputList = new ArrayList<File>();
	static File outPath = null;
	static File umlsIndex = null;
		
	public static int init_clamp( String[] argv ) throws ParseException, InvalidXMLException, ResourceInitializationException, org.apache.commons.cli.ParseException {
		
    	CommandLineParser parser = new BasicParser();
		Options options = new Options();    	

		options.addOption("i", "inputPath", true, "inputPath");
		options.addOption("o", "outputPath", true, "outputPath");
		options.addOption("p", "piepline", true, "clamp pipeline file");
        options.addOption("I", "umlsIndex", true, "umls index dir");
		
		CommandLine commandLine = parser.parse(options, argv);
		String inDir = "";
		if (commandLine.hasOption("i")) {
            inDir = commandLine.getOptionValue("i");
		}
		
		String outDir = "";
		if (commandLine.hasOption("o")) {
            outDir = commandLine.getOptionValue("o");
		}
		
		String pipeline_fn = "";
		if (commandLine.hasOption("p")) {
            pipeline_fn = commandLine.getOptionValue("p");
		}
		
		String umlsIndexDir = "";
    	if (commandLine.hasOption("I")) {
	            umlsIndexDir = commandLine.getOptionValue("I");
		}
	

		// check the input folder;		
		File inPath = new File( inDir );
		if( !inPath.exists() ) {
			System.out.println( "input folder doesn't exist. input=[" + inDir + "]" );
			return -1;
		} else if( !inPath.isDirectory() ) {
			System.out.println( "input should be a folder. input=[" + inDir + "]" );
			return -1;
		}
		for( File file : inPath.listFiles() ) {
			if( file.getName().startsWith( "." ) || !file.getName().endsWith( ".txt" ) ) {
				continue;
			} else {
				inputList.add( file ); 
			}
		}
		System.out.println( "input files count=[" + inputList.size() + "]");

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
		File pipelineJar = new File( pipeline_fn );
		if( !pipelineJar.exists() ) {
			System.out.println( "pipeline jar doesn't exist. pipelineJar=[" + pipeline_fn + "]" );
			return -1;
		} else if( !pipelineJar.isFile() ) {
			System.out.println( "pipeline jar should be a jar file or a .pipefile. pipelineJar=[" + pipeline_fn + "]" );
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
				System.out.println( "pipeline jar should be a jar file or a .pipefile. pipelineJar=[" + pipeline_fn + "]" );
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
		
		
		return 0;
	}
	
	public static void run_pipeline() {   	
    	for( File file : inputList ) {
    		try {
	    		Document doc = new Document( file );
	    		for( DocProcessor proc : procList ) {
	    			try {
						proc.process( doc );
					} catch (AnalysisEngineProcessException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
	    		}
	    		// java File object
	    		doc.save( outPath.getAbsolutePath() + File.separator + file.getName().replace( ".txt", ".xmi" ) );
    		
				FileWriter outfile = new FileWriter( new File( outPath.getAbsolutePath() + File.separator + file.getName() ) );
				for( ClampNameEntity cne : doc.getNameEntity() ) {
					outfile.write( "NamedEntity\t" + cne.getBegin() + "\t" + cne.getEnd() + "\tsemantic=" + cne.getSemanticTag() );
					if( cne.getAssertion() != null && !cne.getAssertion().isEmpty() ) {
						outfile.write( "\tassertion=" + cne.getAssertion() );
					}
					if( cne.getUmlsCui() != null && !cne.getUmlsCui().isEmpty() ) {
						outfile.write( "\tcui=" + cne.getUmlsCui() );
					}
					/*if( cne.getAttr1() != null && !cne.getAttr1().isEmpty() ) {
						outfile.write( "\tattr1=" + cne.getAttr1() );
					}
					if( cne.getAttr2() != null && !cne.getAttr2().isEmpty() ) {
						outfile.write( "\tattr2=" + cne.getAttr2() );
					}
					if( cne.getAttr3() != null && !cne.getAttr3().isEmpty() ) {
						outfile.write( "\tattr3=" + cne.getAttr3() );
					}
					if( cne.getAttr4() != null && !cne.getAttr4().isEmpty() ) {
						outfile.write( "\tattr4=" + cne.getAttr4() );
					}
					if( cne.getAttr( "sentence_prob" ) != null ) {
						outfile.write( "\tsentProb=" + cne.getAttr( "sentence_prob" ) );
					}
					if( cne.getAttr( "concept_prob" ) != null ) {
						outfile.write( "\tconceptProb=" + cne.getAttr( "concept_prob" ) );
					}*/
					outfile.write( "\tne=" + cne.textStr().replace( "\r\n", " " ).replace( "\n", " ") );
					outfile.write( "\n" );
				}
				

				outfile.close();
			} catch (IOException | UIMAException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (DocumentIOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
    		
    		System.out.println( file.getName() + " processed.." );
    	}

		
	}
    
    public static void main( String[] argv ) throws InvalidXMLException, ResourceInitializationException, ParseException, org.apache.commons.cli.ParseException {
             // -i %input% -o %output% -p %pipeline% -A %umlsAPI% -I  %umlsIndex%
		init_clamp(argv);
        run_pipeline();
        System.out.println("Run CLAMP pipeline done.");
    }
}

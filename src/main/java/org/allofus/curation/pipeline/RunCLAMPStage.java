

package org.allofus.curation.pipeline;

import edu.uth.clamp.config.ConfigUtil;
import edu.uth.clamp.config.ConfigurationException;
import edu.uth.clamp.io.DocumentIOException;
import edu.uth.clamp.nlp.encoding.MaxMatchingUmlsEncoderCovid;
import edu.uth.clamp.nlp.encoding.RxNormEncoderUIMA;
import edu.uth.clamp.nlp.structure.ClampNameEntity;
import edu.uth.clamp.nlp.structure.Document;
import edu.uth.clamp.nlp.uima.DocProcessor;
import edu.uth.clamp.nlp.uima.UmlsEncoderUIMA;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.uima.UIMAException;
import org.apache.uima.analysis_engine.AnalysisEngineProcessException;
import org.apache.uima.resource.ResourceInitializationException;
import org.apache.uima.util.InvalidXMLException;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;


public class RunCLAMPStage extends DoFn<String, String> {
	private final Counter emptyLines = Metrics.counter(ExtractWordsFn.class, "emptyLines");
	private final Distribution lineLenDist = Metrics.distribution(ExtractWordsFn.class, "lineLenDistro");
	static List<DocProcessor> procList = new ArrayList<DocProcessor>();
	static List<File> inputList = new ArrayList<File>();
	static File outPath = null;
	static File umlsIndex = null;

	static String clamp_home = "";

	public static int init_clamp( String[] argv ) throws ParseException, InvalidXMLException, ResourceInitializationException, org.apache.commons.cli.ParseException {

		String umlsIndexDir = clamp_home + "/resource/umls_index";
		umlsIndex = new File( umlsIndexDir );
		if( !umlsIndex.exists() ) {
			System.out.println( "cannot find umls index directory. index=[" + umlsIndexDir + "]" );
			return -1;
		} else if( !umlsIndex.isDirectory() ) {
			System.out.println( "umls index should be a directory. index=[" + umlsIndexDir + "]" );
			return -1;
		}

		// pipeline
		String pipeline_fn = clamp_home + "/pipeline/clamp-ner.pipeline.jar";
		File pipelineJar = new File(pipeline_fn);
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

	@ProcessElement
	public void processElement(@Element String element, OutputReceiver<String> receiver) {
		try {
			File file = new File(element);
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
				if( cne.getUmlsCui() != null && !cne.getUmlsCui().isEmpty() ) {
					receiver.output( "\tcui=" + cne.getUmlsCui() );
				}
				receiver.output( "\tne=" + cne.textStr().replace( "\r\n", " " ).replace( "\n", " ") );
//				outfile.write( "\n" );
			}


			outfile.close();
		} catch (IOException | UIMAException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (DocumentIOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		System.out.println( "document processed.." );
	}
}

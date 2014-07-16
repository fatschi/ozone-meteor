// $ANTLR 3.4 /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g 2014-07-14 11:47:03
 
package eu.stratosphere.meteor; 

import eu.stratosphere.sopremo.operator.*;
import eu.stratosphere.sopremo.io.*;
import eu.stratosphere.sopremo.query.*;
import eu.stratosphere.sopremo.pact.*;
import eu.stratosphere.sopremo.expressions.*;
import eu.stratosphere.sopremo.function.*;
import eu.stratosphere.sopremo.type.*;
import java.math.*;
import java.util.IdentityHashMap;


import org.antlr.runtime.*;
import java.util.Stack;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;

import org.antlr.runtime.tree.*;


@SuppressWarnings({"all", "warnings", "unchecked"})
public class MeteorParser extends MeteorParserBase {
    public static final String[] tokenNames = new String[] {
        "<invalid>", "<EOR>", "<DOWN>", "<UP>", "AND", "APOSTROPHE", "COMMENT", "DECIMAL", "DIGIT", "ELSE", "ESC_SEQ", "EXPONENT", "EXPRESSION", "FN", "HEX_DIGIT", "ID", "IF", "IN", "INCLUDE", "INTEGER", "JAVAUDF", "LOWER_LETTER", "NOT", "OCTAL_ESC", "OPERATOR", "OR", "QUOTATION", "SIGN", "SLASH", "STAR", "STRING", "UINT", "UNICODE_ESC", "UPPER_LETTER", "VAR", "WS", "'!'", "'!='", "'\"'", "'&&'", "'&'", "'('", "')'", "'+'", "'++'", "','", "'-'", "'--'", "'.'", "':'", "';'", "'<'", "'<='", "'='", "'=='", "'>'", "'>='", "'?'", "'?.'", "'['", "']'", "'false'", "'null'", "'read'", "'true'", "'using'", "'write'", "'{'", "'||'", "'}'", "'~'"
    };

    public static final int EOF=-1;
    public static final int T__36=36;
    public static final int T__37=37;
    public static final int T__38=38;
    public static final int T__39=39;
    public static final int T__40=40;
    public static final int T__41=41;
    public static final int T__42=42;
    public static final int T__43=43;
    public static final int T__44=44;
    public static final int T__45=45;
    public static final int T__46=46;
    public static final int T__47=47;
    public static final int T__48=48;
    public static final int T__49=49;
    public static final int T__50=50;
    public static final int T__51=51;
    public static final int T__52=52;
    public static final int T__53=53;
    public static final int T__54=54;
    public static final int T__55=55;
    public static final int T__56=56;
    public static final int T__57=57;
    public static final int T__58=58;
    public static final int T__59=59;
    public static final int T__60=60;
    public static final int T__61=61;
    public static final int T__62=62;
    public static final int T__63=63;
    public static final int T__64=64;
    public static final int T__65=65;
    public static final int T__66=66;
    public static final int T__67=67;
    public static final int T__68=68;
    public static final int T__69=69;
    public static final int T__70=70;
    public static final int AND=4;
    public static final int APOSTROPHE=5;
    public static final int COMMENT=6;
    public static final int DECIMAL=7;
    public static final int DIGIT=8;
    public static final int ELSE=9;
    public static final int ESC_SEQ=10;
    public static final int EXPONENT=11;
    public static final int EXPRESSION=12;
    public static final int FN=13;
    public static final int HEX_DIGIT=14;
    public static final int ID=15;
    public static final int IF=16;
    public static final int IN=17;
    public static final int INCLUDE=18;
    public static final int INTEGER=19;
    public static final int JAVAUDF=20;
    public static final int LOWER_LETTER=21;
    public static final int NOT=22;
    public static final int OCTAL_ESC=23;
    public static final int OPERATOR=24;
    public static final int OR=25;
    public static final int QUOTATION=26;
    public static final int SIGN=27;
    public static final int SLASH=28;
    public static final int STAR=29;
    public static final int STRING=30;
    public static final int UINT=31;
    public static final int UNICODE_ESC=32;
    public static final int UPPER_LETTER=33;
    public static final int VAR=34;
    public static final int WS=35;

    // delegates
    public MeteorParserBase[] getDelegates() {
        return new MeteorParserBase[] {};
    }

    // delegators


    public MeteorParser(TokenStream input) {
        this(input, new RecognizerSharedState());
    }
    public MeteorParser(TokenStream input, RecognizerSharedState state) {
        super(input, state);
    }

protected TreeAdaptor adaptor = new CommonTreeAdaptor();

public void setTreeAdaptor(TreeAdaptor adaptor) {
    this.adaptor = adaptor;
}
public TreeAdaptor getTreeAdaptor() {
    return adaptor;
}
    public String[] getTokenNames() { return MeteorParser.tokenNames; }
    public String getGrammarFileName() { return "/home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g"; }


      private Stack<String> paraphrase = new Stack<String>();

      private boolean setInnerOutput(Token VAR, Operator<?> op) {
    	  JsonStreamExpression output = new JsonStreamExpression(op.getOutput(((objectCreation_scope)objectCreation_stack.peek()).mappings.size()));
    	  ((objectCreation_scope)objectCreation_stack.peek()).mappings.add(new ObjectCreation.SymbolicAssignment(output, new JsonStreamExpression(op)));
    	  getVariableRegistry().getRegistry(1).put(VAR.getText(), output);
    	  return true;
    	}
      
      protected EvaluationExpression getInputSelection(Token inputVar) throws RecognitionException {
          return getVariableSafely(inputVar).toInputSelection(((operator_scope)operator_stack.peek()).result);
      }

      public void parseSinks() throws RecognitionException {
        script();
      }


    public static class script_return extends ParserRuleReturnScope {
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };


    // $ANTLR start "script"
    // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:97:1: script : ( statement ';' )+ ->;
    public final MeteorParser.script_return script() throws RecognitionException {
        MeteorParser.script_return retval = new MeteorParser.script_return();
        retval.start = input.LT(1);


        EvaluationExpression root_0 = null;

        Token char_literal2=null;
        MeteorParser.statement_return statement1 =null;


        EvaluationExpression char_literal2_tree=null;
        RewriteRuleTokenStream stream_50=new RewriteRuleTokenStream(adaptor,"token 50");
        RewriteRuleSubtreeStream stream_statement=new RewriteRuleSubtreeStream(adaptor,"rule statement");
        try {
            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:98:2: ( ( statement ';' )+ ->)
            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:98:5: ( statement ';' )+
            {
            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:98:5: ( statement ';' )+
            int cnt1=0;
            loop1:
            do {
                int alt1=2;
                int LA1_0 = input.LA(1);

                if ( (LA1_0==ID||LA1_0==VAR||LA1_0==50||LA1_0==63||(LA1_0 >= 65 && LA1_0 <= 66)) ) {
                    alt1=1;
                }


                switch (alt1) {
            	case 1 :
            	    // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:98:6: statement ';'
            	    {
            	    pushFollow(FOLLOW_statement_in_script131);
            	    statement1=statement();

            	    state._fsp--;
            	    if (state.failed) return retval;
            	    if ( state.backtracking==0 ) stream_statement.add(statement1.getTree());

            	    char_literal2=(Token)match(input,50,FOLLOW_50_in_script133); if (state.failed) return retval; 
            	    if ( state.backtracking==0 ) stream_50.add(char_literal2);


            	    }
            	    break;

            	default :
            	    if ( cnt1 >= 1 ) break loop1;
            	    if (state.backtracking>0) {state.failed=true; return retval;}
                        EarlyExitException eee =
                            new EarlyExitException(1, input);
                        throw eee;
                }
                cnt1++;
            } while (true);


            // AST REWRITE
            // elements: 
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            if ( state.backtracking==0 ) {

            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (EvaluationExpression)adaptor.nil();
            // 98:22: ->
            {
                root_0 = null;
            }


            retval.tree = root_0;
            }

            }

            retval.stop = input.LT(-1);


            if ( state.backtracking==0 ) {

            retval.tree = (EvaluationExpression)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }

        catch (RecognitionException e) {
          throw e;
        }

        finally {
        	// do for sure before leaving
        }
        return retval;
    }
    // $ANTLR end "script"


    public static class statement_return extends ParserRuleReturnScope {
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };


    // $ANTLR start "statement"
    // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:100:1: statement : ( operator | packageImport | adhocSource | definition ||m= functionCall ) ->;
    public final MeteorParser.statement_return statement() throws RecognitionException {
        MeteorParser.statement_return retval = new MeteorParser.statement_return();
        retval.start = input.LT(1);


        EvaluationExpression root_0 = null;

        MeteorParser.functionCall_return m =null;

        MeteorParser.operator_return operator3 =null;

        MeteorParser.packageImport_return packageImport4 =null;

        MeteorParser.adhocSource_return adhocSource5 =null;

        MeteorParser.definition_return definition6 =null;


        RewriteRuleSubtreeStream stream_functionCall=new RewriteRuleSubtreeStream(adaptor,"rule functionCall");
        RewriteRuleSubtreeStream stream_definition=new RewriteRuleSubtreeStream(adaptor,"rule definition");
        RewriteRuleSubtreeStream stream_adhocSource=new RewriteRuleSubtreeStream(adaptor,"rule adhocSource");
        RewriteRuleSubtreeStream stream_operator=new RewriteRuleSubtreeStream(adaptor,"rule operator");
        RewriteRuleSubtreeStream stream_packageImport=new RewriteRuleSubtreeStream(adaptor,"rule packageImport");
        try {
            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:101:2: ( ( operator | packageImport | adhocSource | definition ||m= functionCall ) ->)
            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:101:4: ( operator | packageImport | adhocSource | definition ||m= functionCall )
            {
            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:101:4: ( operator | packageImport | adhocSource | definition ||m= functionCall )
            int alt2=6;
            switch ( input.LA(1) ) {
            case VAR:
                {
                int LA2_1 = input.LA(2);

                if ( (LA2_1==53) ) {
                    int LA2_6 = input.LA(3);

                    if ( (LA2_6==ID||LA2_6==63) ) {
                        alt2=1;
                    }
                    else if ( (LA2_6==59) ) {
                        alt2=3;
                    }
                    else {
                        if (state.backtracking>0) {state.failed=true; return retval;}
                        NoViableAltException nvae =
                            new NoViableAltException("", 2, 6, input);

                        throw nvae;

                    }
                }
                else if ( (LA2_1==45) ) {
                    alt2=1;
                }
                else {
                    if (state.backtracking>0) {state.failed=true; return retval;}
                    NoViableAltException nvae =
                        new NoViableAltException("", 2, 1, input);

                    throw nvae;

                }
                }
                break;
            case 63:
            case 66:
                {
                alt2=1;
                }
                break;
            case ID:
                {
                switch ( input.LA(2) ) {
                case 49:
                    {
                    int LA2_7 = input.LA(3);

                    if ( (LA2_7==ID) ) {
                        int LA2_11 = input.LA(4);

                        if ( (LA2_11==41) ) {
                            alt2=6;
                        }
                        else if ( (LA2_11==ID||LA2_11==VAR||LA2_11==50) ) {
                            alt2=1;
                        }
                        else {
                            if (state.backtracking>0) {state.failed=true; return retval;}
                            NoViableAltException nvae =
                                new NoViableAltException("", 2, 11, input);

                            throw nvae;

                        }
                    }
                    else {
                        if (state.backtracking>0) {state.failed=true; return retval;}
                        NoViableAltException nvae =
                            new NoViableAltException("", 2, 7, input);

                        throw nvae;

                    }
                    }
                    break;
                case 53:
                    {
                    alt2=4;
                    }
                    break;
                case 41:
                    {
                    alt2=6;
                    }
                    break;
                case ID:
                case VAR:
                case 50:
                    {
                    alt2=1;
                    }
                    break;
                default:
                    if (state.backtracking>0) {state.failed=true; return retval;}
                    NoViableAltException nvae =
                        new NoViableAltException("", 2, 3, input);

                    throw nvae;

                }

                }
                break;
            case 65:
                {
                alt2=2;
                }
                break;
            case 50:
                {
                alt2=5;
                }
                break;
            default:
                if (state.backtracking>0) {state.failed=true; return retval;}
                NoViableAltException nvae =
                    new NoViableAltException("", 2, 0, input);

                throw nvae;

            }

            switch (alt2) {
                case 1 :
                    // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:101:5: operator
                    {
                    pushFollow(FOLLOW_operator_in_statement147);
                    operator3=operator();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_operator.add(operator3.getTree());

                    }
                    break;
                case 2 :
                    // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:101:16: packageImport
                    {
                    pushFollow(FOLLOW_packageImport_in_statement151);
                    packageImport4=packageImport();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_packageImport.add(packageImport4.getTree());

                    }
                    break;
                case 3 :
                    // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:101:32: adhocSource
                    {
                    pushFollow(FOLLOW_adhocSource_in_statement155);
                    adhocSource5=adhocSource();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_adhocSource.add(adhocSource5.getTree());

                    }
                    break;
                case 4 :
                    // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:101:46: definition
                    {
                    pushFollow(FOLLOW_definition_in_statement159);
                    definition6=definition();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_definition.add(definition6.getTree());

                    }
                    break;
                case 5 :
                    // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:103:2: 
                    {
                    }
                    break;
                case 6 :
                    // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:103:4: m= functionCall
                    {
                    pushFollow(FOLLOW_functionCall_in_statement169);
                    m=functionCall();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_functionCall.add(m.getTree());

                    if ( state.backtracking==0 ) { (m!=null?((EvaluationExpression)m.tree):null).evaluate(MissingNode.getInstance()); }

                    }
                    break;

            }


            // AST REWRITE
            // elements: 
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            if ( state.backtracking==0 ) {

            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (EvaluationExpression)adaptor.nil();
            // 103:69: ->
            {
                root_0 = null;
            }


            retval.tree = root_0;
            }

            }

            retval.stop = input.LT(-1);


            if ( state.backtracking==0 ) {

            retval.tree = (EvaluationExpression)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }

        catch (RecognitionException e) {
          throw e;
        }

        finally {
        	// do for sure before leaving
        }
        return retval;
    }
    // $ANTLR end "statement"


    public static class packageImport_return extends ParserRuleReturnScope {
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };


    // $ANTLR start "packageImport"
    // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:105:1: packageImport : 'using' packageName= ID ( ',' additionalPackage= ID )* ->;
    public final MeteorParser.packageImport_return packageImport() throws RecognitionException {
        MeteorParser.packageImport_return retval = new MeteorParser.packageImport_return();
        retval.start = input.LT(1);


        EvaluationExpression root_0 = null;

        Token packageName=null;
        Token additionalPackage=null;
        Token string_literal7=null;
        Token char_literal8=null;

        EvaluationExpression packageName_tree=null;
        EvaluationExpression additionalPackage_tree=null;
        EvaluationExpression string_literal7_tree=null;
        EvaluationExpression char_literal8_tree=null;
        RewriteRuleTokenStream stream_45=new RewriteRuleTokenStream(adaptor,"token 45");
        RewriteRuleTokenStream stream_ID=new RewriteRuleTokenStream(adaptor,"token ID");
        RewriteRuleTokenStream stream_65=new RewriteRuleTokenStream(adaptor,"token 65");

        try {
            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:106:3: ( 'using' packageName= ID ( ',' additionalPackage= ID )* ->)
            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:106:6: 'using' packageName= ID ( ',' additionalPackage= ID )*
            {
            string_literal7=(Token)match(input,65,FOLLOW_65_in_packageImport186); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_65.add(string_literal7);


            packageName=(Token)match(input,ID,FOLLOW_ID_in_packageImport190); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_ID.add(packageName);


            if ( state.backtracking==0 ) { getPackageManager().importPackage((packageName!=null?packageName.getText():null)); }

            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:107:6: ( ',' additionalPackage= ID )*
            loop3:
            do {
                int alt3=2;
                int LA3_0 = input.LA(1);

                if ( (LA3_0==45) ) {
                    alt3=1;
                }


                switch (alt3) {
            	case 1 :
            	    // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:107:7: ',' additionalPackage= ID
            	    {
            	    char_literal8=(Token)match(input,45,FOLLOW_45_in_packageImport201); if (state.failed) return retval; 
            	    if ( state.backtracking==0 ) stream_45.add(char_literal8);


            	    additionalPackage=(Token)match(input,ID,FOLLOW_ID_in_packageImport205); if (state.failed) return retval; 
            	    if ( state.backtracking==0 ) stream_ID.add(additionalPackage);


            	    if ( state.backtracking==0 ) { getPackageManager().importPackage((additionalPackage!=null?additionalPackage.getText():null)); }

            	    }
            	    break;

            	default :
            	    break loop3;
                }
            } while (true);


            // AST REWRITE
            // elements: 
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            if ( state.backtracking==0 ) {

            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (EvaluationExpression)adaptor.nil();
            // 107:98: ->
            {
                root_0 = null;
            }


            retval.tree = root_0;
            }

            }

            retval.stop = input.LT(-1);


            if ( state.backtracking==0 ) {

            retval.tree = (EvaluationExpression)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }

        catch (RecognitionException e) {
          throw e;
        }

        finally {
        	// do for sure before leaving
        }
        return retval;
    }
    // $ANTLR end "packageImport"


    public static class definition_return extends ParserRuleReturnScope {
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };


    // $ANTLR start "definition"
    // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:109:1: definition : ( ( ID '=' FN )=> functionDefinition | ( ID '=' JAVAUDF )=> javaudf | constantDefinition );
    public final MeteorParser.definition_return definition() throws RecognitionException {
        MeteorParser.definition_return retval = new MeteorParser.definition_return();
        retval.start = input.LT(1);


        EvaluationExpression root_0 = null;

        MeteorParser.functionDefinition_return functionDefinition9 =null;

        MeteorParser.javaudf_return javaudf10 =null;

        MeteorParser.constantDefinition_return constantDefinition11 =null;



        try {
            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:110:3: ( ( ID '=' FN )=> functionDefinition | ( ID '=' JAVAUDF )=> javaudf | constantDefinition )
            int alt4=3;
            int LA4_0 = input.LA(1);

            if ( (LA4_0==ID) ) {
                int LA4_1 = input.LA(2);

                if ( (synpred1_Meteor()) ) {
                    alt4=1;
                }
                else if ( (synpred2_Meteor()) ) {
                    alt4=2;
                }
                else if ( (true) ) {
                    alt4=3;
                }
                else {
                    if (state.backtracking>0) {state.failed=true; return retval;}
                    NoViableAltException nvae =
                        new NoViableAltException("", 4, 1, input);

                    throw nvae;

                }
            }
            else {
                if (state.backtracking>0) {state.failed=true; return retval;}
                NoViableAltException nvae =
                    new NoViableAltException("", 4, 0, input);

                throw nvae;

            }
            switch (alt4) {
                case 1 :
                    // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:110:5: ( ID '=' FN )=> functionDefinition
                    {
                    root_0 = (EvaluationExpression)adaptor.nil();


                    pushFollow(FOLLOW_functionDefinition_in_definition230);
                    functionDefinition9=functionDefinition();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, functionDefinition9.getTree());

                    }
                    break;
                case 2 :
                    // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:111:5: ( ID '=' JAVAUDF )=> javaudf
                    {
                    root_0 = (EvaluationExpression)adaptor.nil();


                    pushFollow(FOLLOW_javaudf_in_definition246);
                    javaudf10=javaudf();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, javaudf10.getTree());

                    }
                    break;
                case 3 :
                    // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:112:5: constantDefinition
                    {
                    root_0 = (EvaluationExpression)adaptor.nil();


                    pushFollow(FOLLOW_constantDefinition_in_definition252);
                    constantDefinition11=constantDefinition();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, constantDefinition11.getTree());

                    }
                    break;

            }
            retval.stop = input.LT(-1);


            if ( state.backtracking==0 ) {

            retval.tree = (EvaluationExpression)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }

        catch (RecognitionException e) {
          throw e;
        }

        finally {
        	// do for sure before leaving
        }
        return retval;
    }
    // $ANTLR end "definition"


    public static class functionDefinition_return extends ParserRuleReturnScope {
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };


    // $ANTLR start "functionDefinition"
    // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:114:1: functionDefinition : name= ID '=' func= inlineFunction ->;
    public final MeteorParser.functionDefinition_return functionDefinition() throws RecognitionException {
        MeteorParser.functionDefinition_return retval = new MeteorParser.functionDefinition_return();
        retval.start = input.LT(1);


        EvaluationExpression root_0 = null;

        Token name=null;
        Token char_literal12=null;
        MeteorParser.inlineFunction_return func =null;


        EvaluationExpression name_tree=null;
        EvaluationExpression char_literal12_tree=null;
        RewriteRuleTokenStream stream_ID=new RewriteRuleTokenStream(adaptor,"token ID");
        RewriteRuleTokenStream stream_53=new RewriteRuleTokenStream(adaptor,"token 53");
        RewriteRuleSubtreeStream stream_inlineFunction=new RewriteRuleSubtreeStream(adaptor,"rule inlineFunction");
        try {
            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:115:3: (name= ID '=' func= inlineFunction ->)
            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:115:5: name= ID '=' func= inlineFunction
            {
            name=(Token)match(input,ID,FOLLOW_ID_in_functionDefinition264); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_ID.add(name);


            char_literal12=(Token)match(input,53,FOLLOW_53_in_functionDefinition266); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_53.add(char_literal12);


            pushFollow(FOLLOW_inlineFunction_in_functionDefinition270);
            func=inlineFunction();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_inlineFunction.add(func.getTree());

            if ( state.backtracking==0 ) { addFunction((name!=null?name.getText():null), (func!=null?func.func:null)); }

            // AST REWRITE
            // elements: 
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            if ( state.backtracking==0 ) {

            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (EvaluationExpression)adaptor.nil();
            // 115:78: ->
            {
                root_0 = null;
            }


            retval.tree = root_0;
            }

            }

            retval.stop = input.LT(-1);


            if ( state.backtracking==0 ) {

            retval.tree = (EvaluationExpression)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }

        catch (RecognitionException e) {
          throw e;
        }

        finally {
        	// do for sure before leaving
        }
        return retval;
    }
    // $ANTLR end "functionDefinition"


    public static class constantDefinition_return extends ParserRuleReturnScope {
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };


    // $ANTLR start "constantDefinition"
    // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:117:1: constantDefinition : name= ID '=' exp= ternaryExpression ->;
    public final MeteorParser.constantDefinition_return constantDefinition() throws RecognitionException {
        MeteorParser.constantDefinition_return retval = new MeteorParser.constantDefinition_return();
        retval.start = input.LT(1);


        EvaluationExpression root_0 = null;

        Token name=null;
        Token char_literal13=null;
        MeteorParser.ternaryExpression_return exp =null;


        EvaluationExpression name_tree=null;
        EvaluationExpression char_literal13_tree=null;
        RewriteRuleTokenStream stream_ID=new RewriteRuleTokenStream(adaptor,"token ID");
        RewriteRuleTokenStream stream_53=new RewriteRuleTokenStream(adaptor,"token 53");
        RewriteRuleSubtreeStream stream_ternaryExpression=new RewriteRuleSubtreeStream(adaptor,"rule ternaryExpression");
        try {
            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:118:3: (name= ID '=' exp= ternaryExpression ->)
            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:118:5: name= ID '=' exp= ternaryExpression
            {
            name=(Token)match(input,ID,FOLLOW_ID_in_constantDefinition289); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_ID.add(name);


            char_literal13=(Token)match(input,53,FOLLOW_53_in_constantDefinition291); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_53.add(char_literal13);


            pushFollow(FOLLOW_ternaryExpression_in_constantDefinition295);
            exp=ternaryExpression();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_ternaryExpression.add(exp.getTree());

            if ( state.backtracking==0 ) { addConstant((name!=null?name.getText():null), (exp!=null?((EvaluationExpression)exp.tree):null)); }

            // AST REWRITE
            // elements: 
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            if ( state.backtracking==0 ) {

            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (EvaluationExpression)adaptor.nil();
            // 118:79: ->
            {
                root_0 = null;
            }


            retval.tree = root_0;
            }

            }

            retval.stop = input.LT(-1);


            if ( state.backtracking==0 ) {

            retval.tree = (EvaluationExpression)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }

        catch (RecognitionException e) {
          throw e;
        }

        finally {
        	// do for sure before leaving
        }
        return retval;
    }
    // $ANTLR end "constantDefinition"


    public static class inlineFunction_return extends ParserRuleReturnScope {
        public ExpressionFunction func;
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };


    // $ANTLR start "inlineFunction"
    // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:120:1: inlineFunction returns [ExpressionFunction func] : FN '(' (param= ID ( ',' param= ID )* )? ')' '{' def= expression '}' ->;
    public final MeteorParser.inlineFunction_return inlineFunction() throws RecognitionException {
        MeteorParser.inlineFunction_return retval = new MeteorParser.inlineFunction_return();
        retval.start = input.LT(1);


        EvaluationExpression root_0 = null;

        Token param=null;
        Token FN14=null;
        Token char_literal15=null;
        Token char_literal16=null;
        Token char_literal17=null;
        Token char_literal18=null;
        Token char_literal19=null;
        MeteorParser.expression_return def =null;


        EvaluationExpression param_tree=null;
        EvaluationExpression FN14_tree=null;
        EvaluationExpression char_literal15_tree=null;
        EvaluationExpression char_literal16_tree=null;
        EvaluationExpression char_literal17_tree=null;
        EvaluationExpression char_literal18_tree=null;
        EvaluationExpression char_literal19_tree=null;
        RewriteRuleTokenStream stream_67=new RewriteRuleTokenStream(adaptor,"token 67");
        RewriteRuleTokenStream stream_69=new RewriteRuleTokenStream(adaptor,"token 69");
        RewriteRuleTokenStream stream_45=new RewriteRuleTokenStream(adaptor,"token 45");
        RewriteRuleTokenStream stream_42=new RewriteRuleTokenStream(adaptor,"token 42");
        RewriteRuleTokenStream stream_FN=new RewriteRuleTokenStream(adaptor,"token FN");
        RewriteRuleTokenStream stream_41=new RewriteRuleTokenStream(adaptor,"token 41");
        RewriteRuleTokenStream stream_ID=new RewriteRuleTokenStream(adaptor,"token ID");
        RewriteRuleSubtreeStream stream_expression=new RewriteRuleSubtreeStream(adaptor,"rule expression");
         List<Token> params = new ArrayList(); 
        try {
            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:122:3: ( FN '(' (param= ID ( ',' param= ID )* )? ')' '{' def= expression '}' ->)
            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:122:5: FN '(' (param= ID ( ',' param= ID )* )? ')' '{' def= expression '}'
            {
            FN14=(Token)match(input,FN,FOLLOW_FN_in_inlineFunction321); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_FN.add(FN14);


            char_literal15=(Token)match(input,41,FOLLOW_41_in_inlineFunction323); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_41.add(char_literal15);


            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:123:3: (param= ID ( ',' param= ID )* )?
            int alt6=2;
            int LA6_0 = input.LA(1);

            if ( (LA6_0==ID) ) {
                alt6=1;
            }
            switch (alt6) {
                case 1 :
                    // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:123:4: param= ID ( ',' param= ID )*
                    {
                    param=(Token)match(input,ID,FOLLOW_ID_in_inlineFunction332); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_ID.add(param);


                    if ( state.backtracking==0 ) { params.add(param); }

                    // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:124:3: ( ',' param= ID )*
                    loop5:
                    do {
                        int alt5=2;
                        int LA5_0 = input.LA(1);

                        if ( (LA5_0==45) ) {
                            alt5=1;
                        }


                        switch (alt5) {
                    	case 1 :
                    	    // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:124:4: ',' param= ID
                    	    {
                    	    char_literal16=(Token)match(input,45,FOLLOW_45_in_inlineFunction339); if (state.failed) return retval; 
                    	    if ( state.backtracking==0 ) stream_45.add(char_literal16);


                    	    param=(Token)match(input,ID,FOLLOW_ID_in_inlineFunction343); if (state.failed) return retval; 
                    	    if ( state.backtracking==0 ) stream_ID.add(param);


                    	    if ( state.backtracking==0 ) { params.add(param); }

                    	    }
                    	    break;

                    	default :
                    	    break loop5;
                        }
                    } while (true);


                    }
                    break;

            }


            char_literal17=(Token)match(input,42,FOLLOW_42_in_inlineFunction354); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_42.add(char_literal17);


            if ( state.backtracking==0 ) { 
                addConstantScope();
                for(int index = 0; index < params.size(); index++) 
                  this.getConstantRegistry().put(params.get(index).getText(), new InputSelection(index)); 
              }

            char_literal18=(Token)match(input,67,FOLLOW_67_in_inlineFunction364); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_67.add(char_literal18);


            pushFollow(FOLLOW_expression_in_inlineFunction368);
            def=expression();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_expression.add(def.getTree());

            char_literal19=(Token)match(input,69,FOLLOW_69_in_inlineFunction370); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_69.add(char_literal19);


            if ( state.backtracking==0 ) { 
                retval.func = new ExpressionFunction(params.size(), (def!=null?((EvaluationExpression)def.tree):null));
                removeConstantScope(); 
              }

            // AST REWRITE
            // elements: 
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            if ( state.backtracking==0 ) {

            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (EvaluationExpression)adaptor.nil();
            // 135:5: ->
            {
                root_0 = null;
            }


            retval.tree = root_0;
            }

            }

            retval.stop = input.LT(-1);


            if ( state.backtracking==0 ) {

            retval.tree = (EvaluationExpression)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }

        catch (RecognitionException e) {
          throw e;
        }

        finally {
        	// do for sure before leaving
        }
        return retval;
    }
    // $ANTLR end "inlineFunction"


    public static class javaudf_return extends ParserRuleReturnScope {
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };


    // $ANTLR start "javaudf"
    // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:137:1: javaudf : name= ID '=' JAVAUDF '(' path= STRING ')' ->;
    public final MeteorParser.javaudf_return javaudf() throws RecognitionException {
        MeteorParser.javaudf_return retval = new MeteorParser.javaudf_return();
        retval.start = input.LT(1);


        EvaluationExpression root_0 = null;

        Token name=null;
        Token path=null;
        Token char_literal20=null;
        Token JAVAUDF21=null;
        Token char_literal22=null;
        Token char_literal23=null;

        EvaluationExpression name_tree=null;
        EvaluationExpression path_tree=null;
        EvaluationExpression char_literal20_tree=null;
        EvaluationExpression JAVAUDF21_tree=null;
        EvaluationExpression char_literal22_tree=null;
        EvaluationExpression char_literal23_tree=null;
        RewriteRuleTokenStream stream_42=new RewriteRuleTokenStream(adaptor,"token 42");
        RewriteRuleTokenStream stream_41=new RewriteRuleTokenStream(adaptor,"token 41");
        RewriteRuleTokenStream stream_ID=new RewriteRuleTokenStream(adaptor,"token ID");
        RewriteRuleTokenStream stream_53=new RewriteRuleTokenStream(adaptor,"token 53");
        RewriteRuleTokenStream stream_JAVAUDF=new RewriteRuleTokenStream(adaptor,"token JAVAUDF");
        RewriteRuleTokenStream stream_STRING=new RewriteRuleTokenStream(adaptor,"token STRING");

        try {
            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:138:3: (name= ID '=' JAVAUDF '(' path= STRING ')' ->)
            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:138:5: name= ID '=' JAVAUDF '(' path= STRING ')'
            {
            name=(Token)match(input,ID,FOLLOW_ID_in_javaudf390); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_ID.add(name);


            char_literal20=(Token)match(input,53,FOLLOW_53_in_javaudf392); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_53.add(char_literal20);


            JAVAUDF21=(Token)match(input,JAVAUDF,FOLLOW_JAVAUDF_in_javaudf394); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_JAVAUDF.add(JAVAUDF21);


            char_literal22=(Token)match(input,41,FOLLOW_41_in_javaudf396); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_41.add(char_literal22);


            path=(Token)match(input,STRING,FOLLOW_STRING_in_javaudf400); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_STRING.add(path);


            char_literal23=(Token)match(input,42,FOLLOW_42_in_javaudf402); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_42.add(char_literal23);


            if ( state.backtracking==0 ) { addFunction(name.getText(), path.getText()); }

            // AST REWRITE
            // elements: 
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            if ( state.backtracking==0 ) {

            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (EvaluationExpression)adaptor.nil();
            // 139:53: ->
            {
                root_0 = null;
            }


            retval.tree = root_0;
            }

            }

            retval.stop = input.LT(-1);


            if ( state.backtracking==0 ) {

            retval.tree = (EvaluationExpression)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }

        catch (RecognitionException e) {
          throw e;
        }

        finally {
        	// do for sure before leaving
        }
        return retval;
    }
    // $ANTLR end "javaudf"


    protected static class contextAwareExpression_scope {
        EvaluationExpression context;
    }
    protected Stack contextAwareExpression_stack = new Stack();


    public static class contextAwareExpression_return extends ParserRuleReturnScope {
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };


    // $ANTLR start "contextAwareExpression"
    // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:141:1: contextAwareExpression[EvaluationExpression contextExpression] : ternaryExpression ;
    public final MeteorParser.contextAwareExpression_return contextAwareExpression(EvaluationExpression contextExpression) throws RecognitionException {
        contextAwareExpression_stack.push(new contextAwareExpression_scope());
        MeteorParser.contextAwareExpression_return retval = new MeteorParser.contextAwareExpression_return();
        retval.start = input.LT(1);


        EvaluationExpression root_0 = null;

        MeteorParser.ternaryExpression_return ternaryExpression24 =null;



         ((contextAwareExpression_scope)contextAwareExpression_stack.peek()).context = contextExpression; 
        try {
            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:144:3: ( ternaryExpression )
            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:144:5: ternaryExpression
            {
            root_0 = (EvaluationExpression)adaptor.nil();


            pushFollow(FOLLOW_ternaryExpression_in_contextAwareExpression430);
            ternaryExpression24=ternaryExpression();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) adaptor.addChild(root_0, ternaryExpression24.getTree());

            }

            retval.stop = input.LT(-1);


            if ( state.backtracking==0 ) {

            retval.tree = (EvaluationExpression)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }

        catch (RecognitionException e) {
          throw e;
        }

        finally {
        	// do for sure before leaving
            contextAwareExpression_stack.pop();
        }
        return retval;
    }
    // $ANTLR end "contextAwareExpression"


    public static class expression_return extends ParserRuleReturnScope {
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };


    // $ANTLR start "expression"
    // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:146:1: expression : ( ( ID ( ID | VAR ) )=> operatorExpression | ternaryExpression );
    public final MeteorParser.expression_return expression() throws RecognitionException {
        MeteorParser.expression_return retval = new MeteorParser.expression_return();
        retval.start = input.LT(1);


        EvaluationExpression root_0 = null;

        MeteorParser.operatorExpression_return operatorExpression25 =null;

        MeteorParser.ternaryExpression_return ternaryExpression26 =null;



        try {
            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:147:3: ( ( ID ( ID | VAR ) )=> operatorExpression | ternaryExpression )
            int alt7=2;
            int LA7_0 = input.LA(1);

            if ( (LA7_0==VAR) ) {
                int LA7_1 = input.LA(2);

                if ( (LA7_1==53) && (synpred3_Meteor())) {
                    alt7=1;
                }
                else if ( (LA7_1==45) ) {
                    int LA7_7 = input.LA(3);

                    if ( (LA7_7==VAR) ) {
                        int LA7_8 = input.LA(4);

                        if ( (synpred3_Meteor()) ) {
                            alt7=1;
                        }
                        else if ( (true) ) {
                            alt7=2;
                        }
                        else {
                            if (state.backtracking>0) {state.failed=true; return retval;}
                            NoViableAltException nvae =
                                new NoViableAltException("", 7, 8, input);

                            throw nvae;

                        }
                    }
                    else if ( (LA7_7==DECIMAL||LA7_7==FN||LA7_7==ID||LA7_7==INTEGER||(LA7_7 >= STRING && LA7_7 <= UINT)||LA7_7==36||LA7_7==38||(LA7_7 >= 40 && LA7_7 <= 41)||LA7_7==44||LA7_7==47||(LA7_7 >= 59 && LA7_7 <= 64)||(LA7_7 >= 66 && LA7_7 <= 67)||(LA7_7 >= 69 && LA7_7 <= 70)) ) {
                        alt7=2;
                    }
                    else {
                        if (state.backtracking>0) {state.failed=true; return retval;}
                        NoViableAltException nvae =
                            new NoViableAltException("", 7, 7, input);

                        throw nvae;

                    }
                }
                else if ( (LA7_1==AND||(LA7_1 >= ID && LA7_1 <= IN)||LA7_1==NOT||LA7_1==OR||(LA7_1 >= SLASH && LA7_1 <= STAR)||LA7_1==37||LA7_1==39||(LA7_1 >= 42 && LA7_1 <= 43)||LA7_1==46||LA7_1==48||(LA7_1 >= 51 && LA7_1 <= 52)||(LA7_1 >= 54 && LA7_1 <= 60)||(LA7_1 >= 68 && LA7_1 <= 69)) ) {
                    alt7=2;
                }
                else {
                    if (state.backtracking>0) {state.failed=true; return retval;}
                    NoViableAltException nvae =
                        new NoViableAltException("", 7, 1, input);

                    throw nvae;

                }
            }
            else if ( (LA7_0==63) && (synpred3_Meteor())) {
                alt7=1;
            }
            else if ( (LA7_0==66) && (synpred3_Meteor())) {
                alt7=1;
            }
            else if ( (LA7_0==ID) ) {
                int LA7_4 = input.LA(2);

                if ( (synpred3_Meteor()) ) {
                    alt7=1;
                }
                else if ( (true) ) {
                    alt7=2;
                }
                else {
                    if (state.backtracking>0) {state.failed=true; return retval;}
                    NoViableAltException nvae =
                        new NoViableAltException("", 7, 4, input);

                    throw nvae;

                }
            }
            else if ( (LA7_0==DECIMAL||LA7_0==FN||LA7_0==INTEGER||(LA7_0 >= STRING && LA7_0 <= UINT)||LA7_0==36||(LA7_0 >= 40 && LA7_0 <= 41)||LA7_0==44||LA7_0==47||LA7_0==59||(LA7_0 >= 61 && LA7_0 <= 62)||LA7_0==64||LA7_0==67||LA7_0==70) ) {
                alt7=2;
            }
            else {
                if (state.backtracking>0) {state.failed=true; return retval;}
                NoViableAltException nvae =
                    new NoViableAltException("", 7, 0, input);

                throw nvae;

            }
            switch (alt7) {
                case 1 :
                    // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:147:5: ( ID ( ID | VAR ) )=> operatorExpression
                    {
                    root_0 = (EvaluationExpression)adaptor.nil();


                    pushFollow(FOLLOW_operatorExpression_in_expression453);
                    operatorExpression25=operatorExpression();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, operatorExpression25.getTree());

                    }
                    break;
                case 2 :
                    // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:148:5: ternaryExpression
                    {
                    root_0 = (EvaluationExpression)adaptor.nil();


                    pushFollow(FOLLOW_ternaryExpression_in_expression459);
                    ternaryExpression26=ternaryExpression();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, ternaryExpression26.getTree());

                    }
                    break;

            }
            retval.stop = input.LT(-1);


            if ( state.backtracking==0 ) {

            retval.tree = (EvaluationExpression)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }

        catch (RecognitionException e) {
          throw e;
        }

        finally {
        	// do for sure before leaving
        }
        return retval;
    }
    // $ANTLR end "expression"


    protected static class ternaryExpression_scope {
        boolean explicitPackageReferencePossible;
    }
    protected Stack ternaryExpression_stack = new Stack();


    public static class ternaryExpression_return extends ParserRuleReturnScope {
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };


    // $ANTLR start "ternaryExpression"
    // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:150:1: ternaryExpression : ( ( orExpression '?' )=>ifClause= orExpression '?' ( ( '(' )=> '(' ifExpr= orExpression ')' |{...}? => (ifExpr= orExpression )? {...}? =>) ':' elseExpr= orExpression -> ^( EXPRESSION[\"TernaryExpression\"] $ifClause) | ( orExpression IF )=>ifExpr2= orExpression IF ifClause2= orExpression -> ^( EXPRESSION[\"TernaryExpression\"] $ifClause2 $ifExpr2) | orExpression );
    public final MeteorParser.ternaryExpression_return ternaryExpression() throws RecognitionException {
        ternaryExpression_stack.push(new ternaryExpression_scope());
        MeteorParser.ternaryExpression_return retval = new MeteorParser.ternaryExpression_return();
        retval.start = input.LT(1);


        EvaluationExpression root_0 = null;

        Token char_literal27=null;
        Token char_literal28=null;
        Token char_literal29=null;
        Token char_literal30=null;
        Token IF31=null;
        MeteorParser.orExpression_return ifClause =null;

        MeteorParser.orExpression_return ifExpr =null;

        MeteorParser.orExpression_return elseExpr =null;

        MeteorParser.orExpression_return ifExpr2 =null;

        MeteorParser.orExpression_return ifClause2 =null;

        MeteorParser.orExpression_return orExpression32 =null;


        EvaluationExpression char_literal27_tree=null;
        EvaluationExpression char_literal28_tree=null;
        EvaluationExpression char_literal29_tree=null;
        EvaluationExpression char_literal30_tree=null;
        EvaluationExpression IF31_tree=null;
        RewriteRuleTokenStream stream_49=new RewriteRuleTokenStream(adaptor,"token 49");
        RewriteRuleTokenStream stream_42=new RewriteRuleTokenStream(adaptor,"token 42");
        RewriteRuleTokenStream stream_57=new RewriteRuleTokenStream(adaptor,"token 57");
        RewriteRuleTokenStream stream_41=new RewriteRuleTokenStream(adaptor,"token 41");
        RewriteRuleTokenStream stream_IF=new RewriteRuleTokenStream(adaptor,"token IF");
        RewriteRuleSubtreeStream stream_orExpression=new RewriteRuleSubtreeStream(adaptor,"rule orExpression");
         ((ternaryExpression_scope)ternaryExpression_stack.peek()).explicitPackageReferencePossible = true; 
        try {
            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:153:2: ( ( orExpression '?' )=>ifClause= orExpression '?' ( ( '(' )=> '(' ifExpr= orExpression ')' |{...}? => (ifExpr= orExpression )? {...}? =>) ':' elseExpr= orExpression -> ^( EXPRESSION[\"TernaryExpression\"] $ifClause) | ( orExpression IF )=>ifExpr2= orExpression IF ifClause2= orExpression -> ^( EXPRESSION[\"TernaryExpression\"] $ifClause2 $ifExpr2) | orExpression )
            int alt10=3;
            switch ( input.LA(1) ) {
            case 44:
                {
                int LA10_1 = input.LA(2);

                if ( (synpred4_Meteor()) ) {
                    alt10=1;
                }
                else if ( (synpred6_Meteor()) ) {
                    alt10=2;
                }
                else if ( (true) ) {
                    alt10=3;
                }
                else {
                    if (state.backtracking>0) {state.failed=true; return retval;}
                    NoViableAltException nvae =
                        new NoViableAltException("", 10, 1, input);

                    throw nvae;

                }
                }
                break;
            case 47:
                {
                int LA10_2 = input.LA(2);

                if ( (synpred4_Meteor()) ) {
                    alt10=1;
                }
                else if ( (synpred6_Meteor()) ) {
                    alt10=2;
                }
                else if ( (true) ) {
                    alt10=3;
                }
                else {
                    if (state.backtracking>0) {state.failed=true; return retval;}
                    NoViableAltException nvae =
                        new NoViableAltException("", 10, 2, input);

                    throw nvae;

                }
                }
                break;
            case 36:
            case 70:
                {
                int LA10_3 = input.LA(2);

                if ( (synpred4_Meteor()) ) {
                    alt10=1;
                }
                else if ( (synpred6_Meteor()) ) {
                    alt10=2;
                }
                else if ( (true) ) {
                    alt10=3;
                }
                else {
                    if (state.backtracking>0) {state.failed=true; return retval;}
                    NoViableAltException nvae =
                        new NoViableAltException("", 10, 3, input);

                    throw nvae;

                }
                }
                break;
            case 41:
                {
                int LA10_4 = input.LA(2);

                if ( (synpred4_Meteor()) ) {
                    alt10=1;
                }
                else if ( (synpred6_Meteor()) ) {
                    alt10=2;
                }
                else if ( (true) ) {
                    alt10=3;
                }
                else {
                    if (state.backtracking>0) {state.failed=true; return retval;}
                    NoViableAltException nvae =
                        new NoViableAltException("", 10, 4, input);

                    throw nvae;

                }
                }
                break;
            case ID:
                {
                int LA10_5 = input.LA(2);

                if ( (synpred4_Meteor()) ) {
                    alt10=1;
                }
                else if ( (synpred6_Meteor()) ) {
                    alt10=2;
                }
                else if ( (true) ) {
                    alt10=3;
                }
                else {
                    if (state.backtracking>0) {state.failed=true; return retval;}
                    NoViableAltException nvae =
                        new NoViableAltException("", 10, 5, input);

                    throw nvae;

                }
                }
                break;
            case 40:
                {
                int LA10_6 = input.LA(2);

                if ( (synpred4_Meteor()) ) {
                    alt10=1;
                }
                else if ( (synpred6_Meteor()) ) {
                    alt10=2;
                }
                else if ( (true) ) {
                    alt10=3;
                }
                else {
                    if (state.backtracking>0) {state.failed=true; return retval;}
                    NoViableAltException nvae =
                        new NoViableAltException("", 10, 6, input);

                    throw nvae;

                }
                }
                break;
            case FN:
                {
                int LA10_7 = input.LA(2);

                if ( (synpred4_Meteor()) ) {
                    alt10=1;
                }
                else if ( (synpred6_Meteor()) ) {
                    alt10=2;
                }
                else if ( (true) ) {
                    alt10=3;
                }
                else {
                    if (state.backtracking>0) {state.failed=true; return retval;}
                    NoViableAltException nvae =
                        new NoViableAltException("", 10, 7, input);

                    throw nvae;

                }
                }
                break;
            case 64:
                {
                int LA10_8 = input.LA(2);

                if ( (synpred4_Meteor()) ) {
                    alt10=1;
                }
                else if ( (synpred6_Meteor()) ) {
                    alt10=2;
                }
                else if ( (true) ) {
                    alt10=3;
                }
                else {
                    if (state.backtracking>0) {state.failed=true; return retval;}
                    NoViableAltException nvae =
                        new NoViableAltException("", 10, 8, input);

                    throw nvae;

                }
                }
                break;
            case 61:
                {
                int LA10_9 = input.LA(2);

                if ( (synpred4_Meteor()) ) {
                    alt10=1;
                }
                else if ( (synpred6_Meteor()) ) {
                    alt10=2;
                }
                else if ( (true) ) {
                    alt10=3;
                }
                else {
                    if (state.backtracking>0) {state.failed=true; return retval;}
                    NoViableAltException nvae =
                        new NoViableAltException("", 10, 9, input);

                    throw nvae;

                }
                }
                break;
            case DECIMAL:
                {
                int LA10_10 = input.LA(2);

                if ( (synpred4_Meteor()) ) {
                    alt10=1;
                }
                else if ( (synpred6_Meteor()) ) {
                    alt10=2;
                }
                else if ( (true) ) {
                    alt10=3;
                }
                else {
                    if (state.backtracking>0) {state.failed=true; return retval;}
                    NoViableAltException nvae =
                        new NoViableAltException("", 10, 10, input);

                    throw nvae;

                }
                }
                break;
            case STRING:
                {
                int LA10_11 = input.LA(2);

                if ( (synpred4_Meteor()) ) {
                    alt10=1;
                }
                else if ( (synpred6_Meteor()) ) {
                    alt10=2;
                }
                else if ( (true) ) {
                    alt10=3;
                }
                else {
                    if (state.backtracking>0) {state.failed=true; return retval;}
                    NoViableAltException nvae =
                        new NoViableAltException("", 10, 11, input);

                    throw nvae;

                }
                }
                break;
            case UINT:
                {
                int LA10_12 = input.LA(2);

                if ( (synpred4_Meteor()) ) {
                    alt10=1;
                }
                else if ( (synpred6_Meteor()) ) {
                    alt10=2;
                }
                else if ( (true) ) {
                    alt10=3;
                }
                else {
                    if (state.backtracking>0) {state.failed=true; return retval;}
                    NoViableAltException nvae =
                        new NoViableAltException("", 10, 12, input);

                    throw nvae;

                }
                }
                break;
            case INTEGER:
                {
                int LA10_13 = input.LA(2);

                if ( (synpred4_Meteor()) ) {
                    alt10=1;
                }
                else if ( (synpred6_Meteor()) ) {
                    alt10=2;
                }
                else if ( (true) ) {
                    alt10=3;
                }
                else {
                    if (state.backtracking>0) {state.failed=true; return retval;}
                    NoViableAltException nvae =
                        new NoViableAltException("", 10, 13, input);

                    throw nvae;

                }
                }
                break;
            case 62:
                {
                int LA10_14 = input.LA(2);

                if ( (synpred4_Meteor()) ) {
                    alt10=1;
                }
                else if ( (synpred6_Meteor()) ) {
                    alt10=2;
                }
                else if ( (true) ) {
                    alt10=3;
                }
                else {
                    if (state.backtracking>0) {state.failed=true; return retval;}
                    NoViableAltException nvae =
                        new NoViableAltException("", 10, 14, input);

                    throw nvae;

                }
                }
                break;
            case VAR:
                {
                int LA10_15 = input.LA(2);

                if ( (synpred4_Meteor()) ) {
                    alt10=1;
                }
                else if ( (synpred6_Meteor()) ) {
                    alt10=2;
                }
                else if ( (true) ) {
                    alt10=3;
                }
                else {
                    if (state.backtracking>0) {state.failed=true; return retval;}
                    NoViableAltException nvae =
                        new NoViableAltException("", 10, 15, input);

                    throw nvae;

                }
                }
                break;
            case 59:
                {
                int LA10_16 = input.LA(2);

                if ( (synpred4_Meteor()) ) {
                    alt10=1;
                }
                else if ( (synpred6_Meteor()) ) {
                    alt10=2;
                }
                else if ( (true) ) {
                    alt10=3;
                }
                else {
                    if (state.backtracking>0) {state.failed=true; return retval;}
                    NoViableAltException nvae =
                        new NoViableAltException("", 10, 16, input);

                    throw nvae;

                }
                }
                break;
            case 67:
                {
                int LA10_17 = input.LA(2);

                if ( (synpred4_Meteor()) ) {
                    alt10=1;
                }
                else if ( (synpred6_Meteor()) ) {
                    alt10=2;
                }
                else if ( (true) ) {
                    alt10=3;
                }
                else {
                    if (state.backtracking>0) {state.failed=true; return retval;}
                    NoViableAltException nvae =
                        new NoViableAltException("", 10, 17, input);

                    throw nvae;

                }
                }
                break;
            default:
                if (state.backtracking>0) {state.failed=true; return retval;}
                NoViableAltException nvae =
                    new NoViableAltException("", 10, 0, input);

                throw nvae;

            }

            switch (alt10) {
                case 1 :
                    // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:153:4: ( orExpression '?' )=>ifClause= orExpression '?' ( ( '(' )=> '(' ifExpr= orExpression ')' |{...}? => (ifExpr= orExpression )? {...}? =>) ':' elseExpr= orExpression
                    {
                    pushFollow(FOLLOW_orExpression_in_ternaryExpression486);
                    ifClause=orExpression();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_orExpression.add(ifClause.getTree());

                    char_literal27=(Token)match(input,57,FOLLOW_57_in_ternaryExpression492); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_57.add(char_literal27);


                    // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:154:8: ( ( '(' )=> '(' ifExpr= orExpression ')' |{...}? => (ifExpr= orExpression )? {...}? =>)
                    int alt9=2;
                    int LA9_0 = input.LA(1);

                    if ( (LA9_0==41) && (( !(((ternaryExpression_scope)ternaryExpression_stack.peek()).explicitPackageReferencePossible = false) ))) {
                        int LA9_1 = input.LA(2);

                        if ( (synpred5_Meteor()) ) {
                            alt9=1;
                        }
                        else if ( (( !(((ternaryExpression_scope)ternaryExpression_stack.peek()).explicitPackageReferencePossible = false) )) ) {
                            alt9=2;
                        }
                        else {
                            if (state.backtracking>0) {state.failed=true; return retval;}
                            NoViableAltException nvae =
                                new NoViableAltException("", 9, 1, input);

                            throw nvae;

                        }
                    }
                    else if ( (LA9_0==DECIMAL||LA9_0==FN||LA9_0==ID||LA9_0==INTEGER||(LA9_0 >= STRING && LA9_0 <= UINT)||LA9_0==VAR||LA9_0==36||LA9_0==40||LA9_0==44||LA9_0==47||LA9_0==59||(LA9_0 >= 61 && LA9_0 <= 62)||LA9_0==64||LA9_0==67||LA9_0==70) && (( !(((ternaryExpression_scope)ternaryExpression_stack.peek()).explicitPackageReferencePossible = false) ))) {
                        alt9=2;
                    }
                    else if ( (LA9_0==49) && (((( (((ternaryExpression_scope)ternaryExpression_stack.peek()).explicitPackageReferencePossible = true) )&&( (((ternaryExpression_scope)ternaryExpression_stack.peek()).explicitPackageReferencePossible = true) ))&&( !(((ternaryExpression_scope)ternaryExpression_stack.peek()).explicitPackageReferencePossible = false) )))) {
                        alt9=2;
                    }
                    else {
                        if (state.backtracking>0) {state.failed=true; return retval;}
                        NoViableAltException nvae =
                            new NoViableAltException("", 9, 0, input);

                        throw nvae;

                    }
                    switch (alt9) {
                        case 1 :
                            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:154:9: ( '(' )=> '(' ifExpr= orExpression ')'
                            {
                            char_literal28=(Token)match(input,41,FOLLOW_41_in_ternaryExpression501); if (state.failed) return retval; 
                            if ( state.backtracking==0 ) stream_41.add(char_literal28);


                            pushFollow(FOLLOW_orExpression_in_ternaryExpression505);
                            ifExpr=orExpression();

                            state._fsp--;
                            if (state.failed) return retval;
                            if ( state.backtracking==0 ) stream_orExpression.add(ifExpr.getTree());

                            char_literal29=(Token)match(input,42,FOLLOW_42_in_ternaryExpression507); if (state.failed) return retval; 
                            if ( state.backtracking==0 ) stream_42.add(char_literal29);


                            }
                            break;
                        case 2 :
                            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:154:48: {...}? => (ifExpr= orExpression )? {...}? =>
                            {
                            if ( !(( !(((ternaryExpression_scope)ternaryExpression_stack.peek()).explicitPackageReferencePossible = false) )) ) {
                                if (state.backtracking>0) {state.failed=true; return retval;}
                                throw new FailedPredicateException(input, "ternaryExpression", " !($ternaryExpression::explicitPackageReferencePossible = false) ");
                            }

                            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:154:125: (ifExpr= orExpression )?
                            int alt8=2;
                            int LA8_0 = input.LA(1);

                            if ( (LA8_0==DECIMAL||LA8_0==FN||LA8_0==ID||LA8_0==INTEGER||(LA8_0 >= STRING && LA8_0 <= UINT)||LA8_0==VAR||LA8_0==36||(LA8_0 >= 40 && LA8_0 <= 41)||LA8_0==44||LA8_0==47||LA8_0==59||(LA8_0 >= 61 && LA8_0 <= 62)||LA8_0==64||LA8_0==67||LA8_0==70) ) {
                                alt8=1;
                            }
                            switch (alt8) {
                                case 1 :
                                    // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:154:125: ifExpr= orExpression
                                    {
                                    pushFollow(FOLLOW_orExpression_in_ternaryExpression516);
                                    ifExpr=orExpression();

                                    state._fsp--;
                                    if (state.failed) return retval;
                                    if ( state.backtracking==0 ) stream_orExpression.add(ifExpr.getTree());

                                    }
                                    break;

                            }


                            if ( !(( (((ternaryExpression_scope)ternaryExpression_stack.peek()).explicitPackageReferencePossible = true) )) ) {
                                if (state.backtracking>0) {state.failed=true; return retval;}
                                throw new FailedPredicateException(input, "ternaryExpression", " ($ternaryExpression::explicitPackageReferencePossible = true) ");
                            }

                            }
                            break;

                    }


                    char_literal30=(Token)match(input,49,FOLLOW_49_in_ternaryExpression527); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_49.add(char_literal30);


                    pushFollow(FOLLOW_orExpression_in_ternaryExpression531);
                    elseExpr=orExpression();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_orExpression.add(elseExpr.getTree());

                    // AST REWRITE
                    // elements: ifClause
                    // token labels: 
                    // rule labels: retval, ifClause
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    if ( state.backtracking==0 ) {

                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);
                    RewriteRuleSubtreeStream stream_ifClause=new RewriteRuleSubtreeStream(adaptor,"rule ifClause",ifClause!=null?ifClause.tree:null);

                    root_0 = (EvaluationExpression)adaptor.nil();
                    // 156:2: -> ^( EXPRESSION[\"TernaryExpression\"] $ifClause)
                    {
                        // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:156:5: ^( EXPRESSION[\"TernaryExpression\"] $ifClause)
                        {
                        EvaluationExpression root_1 = (EvaluationExpression)adaptor.nil();
                        root_1 = (EvaluationExpression)adaptor.becomeRoot(
                        (EvaluationExpression)adaptor.create(EXPRESSION, "TernaryExpression")
                        , root_1);

                        adaptor.addChild(root_1, stream_ifClause.nextTree());

                        adaptor.addChild(root_1,  ifExpr == null ? (ifClause!=null?((EvaluationExpression)ifClause.tree):null) : (ifExpr!=null?((EvaluationExpression)ifExpr.tree):null) );

                        adaptor.addChild(root_1,  (elseExpr!=null?((EvaluationExpression)elseExpr.tree):null) );

                        adaptor.addChild(root_0, root_1);
                        }

                    }


                    retval.tree = root_0;
                    }

                    }
                    break;
                case 2 :
                    // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:157:4: ( orExpression IF )=>ifExpr2= orExpression IF ifClause2= orExpression
                    {
                    pushFollow(FOLLOW_orExpression_in_ternaryExpression560);
                    ifExpr2=orExpression();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_orExpression.add(ifExpr2.getTree());

                    IF31=(Token)match(input,IF,FOLLOW_IF_in_ternaryExpression562); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_IF.add(IF31);


                    pushFollow(FOLLOW_orExpression_in_ternaryExpression566);
                    ifClause2=orExpression();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_orExpression.add(ifClause2.getTree());

                    // AST REWRITE
                    // elements: ifExpr2, ifClause2
                    // token labels: 
                    // rule labels: retval, ifExpr2, ifClause2
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    if ( state.backtracking==0 ) {

                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);
                    RewriteRuleSubtreeStream stream_ifExpr2=new RewriteRuleSubtreeStream(adaptor,"rule ifExpr2",ifExpr2!=null?ifExpr2.tree:null);
                    RewriteRuleSubtreeStream stream_ifClause2=new RewriteRuleSubtreeStream(adaptor,"rule ifClause2",ifClause2!=null?ifClause2.tree:null);

                    root_0 = (EvaluationExpression)adaptor.nil();
                    // 158:3: -> ^( EXPRESSION[\"TernaryExpression\"] $ifClause2 $ifExpr2)
                    {
                        // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:158:6: ^( EXPRESSION[\"TernaryExpression\"] $ifClause2 $ifExpr2)
                        {
                        EvaluationExpression root_1 = (EvaluationExpression)adaptor.nil();
                        root_1 = (EvaluationExpression)adaptor.becomeRoot(
                        (EvaluationExpression)adaptor.create(EXPRESSION, "TernaryExpression")
                        , root_1);

                        adaptor.addChild(root_1, stream_ifClause2.nextTree());

                        adaptor.addChild(root_1, stream_ifExpr2.nextTree());

                        adaptor.addChild(root_1,  EvaluationExpression.VALUE );

                        adaptor.addChild(root_0, root_1);
                        }

                    }


                    retval.tree = root_0;
                    }

                    }
                    break;
                case 3 :
                    // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:159:5: orExpression
                    {
                    root_0 = (EvaluationExpression)adaptor.nil();


                    pushFollow(FOLLOW_orExpression_in_ternaryExpression589);
                    orExpression32=orExpression();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, orExpression32.getTree());

                    }
                    break;

            }
            retval.stop = input.LT(-1);


            if ( state.backtracking==0 ) {

            retval.tree = (EvaluationExpression)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }

        catch (RecognitionException e) {
          throw e;
        }

        finally {
        	// do for sure before leaving
            ternaryExpression_stack.pop();
        }
        return retval;
    }
    // $ANTLR end "ternaryExpression"


    public static class orExpression_return extends ParserRuleReturnScope {
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };


    // $ANTLR start "orExpression"
    // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:161:1: orExpression :exprs+= andExpression ( ( OR | '||' ) exprs+= andExpression )* -> { $exprs.size() == 1 }? ->;
    public final MeteorParser.orExpression_return orExpression() throws RecognitionException {
        MeteorParser.orExpression_return retval = new MeteorParser.orExpression_return();
        retval.start = input.LT(1);


        EvaluationExpression root_0 = null;

        Token OR33=null;
        Token string_literal34=null;
        List list_exprs=null;
        RuleReturnScope exprs = null;
        EvaluationExpression OR33_tree=null;
        EvaluationExpression string_literal34_tree=null;
        RewriteRuleTokenStream stream_68=new RewriteRuleTokenStream(adaptor,"token 68");
        RewriteRuleTokenStream stream_OR=new RewriteRuleTokenStream(adaptor,"token OR");
        RewriteRuleSubtreeStream stream_andExpression=new RewriteRuleSubtreeStream(adaptor,"rule andExpression");
        try {
            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:162:3: (exprs+= andExpression ( ( OR | '||' ) exprs+= andExpression )* -> { $exprs.size() == 1 }? ->)
            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:162:5: exprs+= andExpression ( ( OR | '||' ) exprs+= andExpression )*
            {
            pushFollow(FOLLOW_andExpression_in_orExpression602);
            exprs=andExpression();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_andExpression.add(exprs.getTree());
            if (list_exprs==null) list_exprs=new ArrayList();
            list_exprs.add(exprs.getTree());


            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:162:26: ( ( OR | '||' ) exprs+= andExpression )*
            loop12:
            do {
                int alt12=2;
                int LA12_0 = input.LA(1);

                if ( (LA12_0==OR||LA12_0==68) ) {
                    alt12=1;
                }


                switch (alt12) {
            	case 1 :
            	    // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:162:27: ( OR | '||' ) exprs+= andExpression
            	    {
            	    // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:162:27: ( OR | '||' )
            	    int alt11=2;
            	    int LA11_0 = input.LA(1);

            	    if ( (LA11_0==OR) ) {
            	        alt11=1;
            	    }
            	    else if ( (LA11_0==68) ) {
            	        alt11=2;
            	    }
            	    else {
            	        if (state.backtracking>0) {state.failed=true; return retval;}
            	        NoViableAltException nvae =
            	            new NoViableAltException("", 11, 0, input);

            	        throw nvae;

            	    }
            	    switch (alt11) {
            	        case 1 :
            	            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:162:28: OR
            	            {
            	            OR33=(Token)match(input,OR,FOLLOW_OR_in_orExpression606); if (state.failed) return retval; 
            	            if ( state.backtracking==0 ) stream_OR.add(OR33);


            	            }
            	            break;
            	        case 2 :
            	            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:162:33: '||'
            	            {
            	            string_literal34=(Token)match(input,68,FOLLOW_68_in_orExpression610); if (state.failed) return retval; 
            	            if ( state.backtracking==0 ) stream_68.add(string_literal34);


            	            }
            	            break;

            	    }


            	    pushFollow(FOLLOW_andExpression_in_orExpression615);
            	    exprs=andExpression();

            	    state._fsp--;
            	    if (state.failed) return retval;
            	    if ( state.backtracking==0 ) stream_andExpression.add(exprs.getTree());
            	    if (list_exprs==null) list_exprs=new ArrayList();
            	    list_exprs.add(exprs.getTree());


            	    }
            	    break;

            	default :
            	    break loop12;
                }
            } while (true);


            // AST REWRITE
            // elements: 
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            if ( state.backtracking==0 ) {

            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (EvaluationExpression)adaptor.nil();
            // 163:3: -> { $exprs.size() == 1 }?
            if ( list_exprs.size() == 1 ) {
                adaptor.addChild(root_0,  list_exprs.get(0) );

            }

            else // 164:3: ->
            {
                adaptor.addChild(root_0,  OrExpression.valueOf((List) list_exprs) );

            }


            retval.tree = root_0;
            }

            }

            retval.stop = input.LT(-1);


            if ( state.backtracking==0 ) {

            retval.tree = (EvaluationExpression)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }

        catch (RecognitionException e) {
          throw e;
        }

        finally {
        	// do for sure before leaving
        }
        return retval;
    }
    // $ANTLR end "orExpression"


    public static class andExpression_return extends ParserRuleReturnScope {
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };


    // $ANTLR start "andExpression"
    // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:166:1: andExpression :exprs+= elementExpression ( ( AND | '&&' ) exprs+= elementExpression )* -> { $exprs.size() == 1 }? ->;
    public final MeteorParser.andExpression_return andExpression() throws RecognitionException {
        MeteorParser.andExpression_return retval = new MeteorParser.andExpression_return();
        retval.start = input.LT(1);


        EvaluationExpression root_0 = null;

        Token AND35=null;
        Token string_literal36=null;
        List list_exprs=null;
        RuleReturnScope exprs = null;
        EvaluationExpression AND35_tree=null;
        EvaluationExpression string_literal36_tree=null;
        RewriteRuleTokenStream stream_AND=new RewriteRuleTokenStream(adaptor,"token AND");
        RewriteRuleTokenStream stream_39=new RewriteRuleTokenStream(adaptor,"token 39");
        RewriteRuleSubtreeStream stream_elementExpression=new RewriteRuleSubtreeStream(adaptor,"rule elementExpression");
        try {
            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:167:3: (exprs+= elementExpression ( ( AND | '&&' ) exprs+= elementExpression )* -> { $exprs.size() == 1 }? ->)
            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:167:5: exprs+= elementExpression ( ( AND | '&&' ) exprs+= elementExpression )*
            {
            pushFollow(FOLLOW_elementExpression_in_andExpression644);
            exprs=elementExpression();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_elementExpression.add(exprs.getTree());
            if (list_exprs==null) list_exprs=new ArrayList();
            list_exprs.add(exprs.getTree());


            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:167:30: ( ( AND | '&&' ) exprs+= elementExpression )*
            loop14:
            do {
                int alt14=2;
                int LA14_0 = input.LA(1);

                if ( (LA14_0==AND||LA14_0==39) ) {
                    alt14=1;
                }


                switch (alt14) {
            	case 1 :
            	    // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:167:31: ( AND | '&&' ) exprs+= elementExpression
            	    {
            	    // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:167:31: ( AND | '&&' )
            	    int alt13=2;
            	    int LA13_0 = input.LA(1);

            	    if ( (LA13_0==AND) ) {
            	        alt13=1;
            	    }
            	    else if ( (LA13_0==39) ) {
            	        alt13=2;
            	    }
            	    else {
            	        if (state.backtracking>0) {state.failed=true; return retval;}
            	        NoViableAltException nvae =
            	            new NoViableAltException("", 13, 0, input);

            	        throw nvae;

            	    }
            	    switch (alt13) {
            	        case 1 :
            	            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:167:32: AND
            	            {
            	            AND35=(Token)match(input,AND,FOLLOW_AND_in_andExpression648); if (state.failed) return retval; 
            	            if ( state.backtracking==0 ) stream_AND.add(AND35);


            	            }
            	            break;
            	        case 2 :
            	            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:167:38: '&&'
            	            {
            	            string_literal36=(Token)match(input,39,FOLLOW_39_in_andExpression652); if (state.failed) return retval; 
            	            if ( state.backtracking==0 ) stream_39.add(string_literal36);


            	            }
            	            break;

            	    }


            	    pushFollow(FOLLOW_elementExpression_in_andExpression657);
            	    exprs=elementExpression();

            	    state._fsp--;
            	    if (state.failed) return retval;
            	    if ( state.backtracking==0 ) stream_elementExpression.add(exprs.getTree());
            	    if (list_exprs==null) list_exprs=new ArrayList();
            	    list_exprs.add(exprs.getTree());


            	    }
            	    break;

            	default :
            	    break loop14;
                }
            } while (true);


            // AST REWRITE
            // elements: 
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            if ( state.backtracking==0 ) {

            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (EvaluationExpression)adaptor.nil();
            // 168:3: -> { $exprs.size() == 1 }?
            if ( list_exprs.size() == 1 ) {
                adaptor.addChild(root_0,  list_exprs.get(0) );

            }

            else // 169:3: ->
            {
                adaptor.addChild(root_0,  AndExpression.valueOf((List) list_exprs) );

            }


            retval.tree = root_0;
            }

            }

            retval.stop = input.LT(-1);


            if ( state.backtracking==0 ) {

            retval.tree = (EvaluationExpression)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }

        catch (RecognitionException e) {
          throw e;
        }

        finally {
        	// do for sure before leaving
        }
        return retval;
    }
    // $ANTLR end "andExpression"


    public static class elementExpression_return extends ParserRuleReturnScope {
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };


    // $ANTLR start "elementExpression"
    // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:171:1: elementExpression : elem= comparisonExpression ( (not= NOT )? IN set= elementExpression )? -> { set == null }? $elem -> ^( EXPRESSION[\"ElementInSetExpression\"] $elem $set) ;
    public final MeteorParser.elementExpression_return elementExpression() throws RecognitionException {
        MeteorParser.elementExpression_return retval = new MeteorParser.elementExpression_return();
        retval.start = input.LT(1);


        EvaluationExpression root_0 = null;

        Token not=null;
        Token IN37=null;
        MeteorParser.comparisonExpression_return elem =null;

        MeteorParser.elementExpression_return set =null;


        EvaluationExpression not_tree=null;
        EvaluationExpression IN37_tree=null;
        RewriteRuleTokenStream stream_IN=new RewriteRuleTokenStream(adaptor,"token IN");
        RewriteRuleTokenStream stream_NOT=new RewriteRuleTokenStream(adaptor,"token NOT");
        RewriteRuleSubtreeStream stream_comparisonExpression=new RewriteRuleSubtreeStream(adaptor,"rule comparisonExpression");
        RewriteRuleSubtreeStream stream_elementExpression=new RewriteRuleSubtreeStream(adaptor,"rule elementExpression");
        try {
            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:172:2: (elem= comparisonExpression ( (not= NOT )? IN set= elementExpression )? -> { set == null }? $elem -> ^( EXPRESSION[\"ElementInSetExpression\"] $elem $set) )
            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:172:4: elem= comparisonExpression ( (not= NOT )? IN set= elementExpression )?
            {
            pushFollow(FOLLOW_comparisonExpression_in_elementExpression686);
            elem=comparisonExpression();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_comparisonExpression.add(elem.getTree());

            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:172:30: ( (not= NOT )? IN set= elementExpression )?
            int alt16=2;
            int LA16_0 = input.LA(1);

            if ( (LA16_0==IN||LA16_0==NOT) ) {
                alt16=1;
            }
            switch (alt16) {
                case 1 :
                    // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:172:31: (not= NOT )? IN set= elementExpression
                    {
                    // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:172:34: (not= NOT )?
                    int alt15=2;
                    int LA15_0 = input.LA(1);

                    if ( (LA15_0==NOT) ) {
                        alt15=1;
                    }
                    switch (alt15) {
                        case 1 :
                            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:172:34: not= NOT
                            {
                            not=(Token)match(input,NOT,FOLLOW_NOT_in_elementExpression691); if (state.failed) return retval; 
                            if ( state.backtracking==0 ) stream_NOT.add(not);


                            }
                            break;

                    }


                    IN37=(Token)match(input,IN,FOLLOW_IN_in_elementExpression694); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_IN.add(IN37);


                    pushFollow(FOLLOW_elementExpression_in_elementExpression698);
                    set=elementExpression();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_elementExpression.add(set.getTree());

                    }
                    break;

            }


            // AST REWRITE
            // elements: set, elem, elem
            // token labels: 
            // rule labels: elem, retval, set
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            if ( state.backtracking==0 ) {

            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_elem=new RewriteRuleSubtreeStream(adaptor,"rule elem",elem!=null?elem.tree:null);
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);
            RewriteRuleSubtreeStream stream_set=new RewriteRuleSubtreeStream(adaptor,"rule set",set!=null?set.tree:null);

            root_0 = (EvaluationExpression)adaptor.nil();
            // 173:2: -> { set == null }? $elem
            if ( set == null ) {
                adaptor.addChild(root_0, stream_elem.nextTree());

            }

            else // 174:2: -> ^( EXPRESSION[\"ElementInSetExpression\"] $elem $set)
            {
                // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:174:5: ^( EXPRESSION[\"ElementInSetExpression\"] $elem $set)
                {
                EvaluationExpression root_1 = (EvaluationExpression)adaptor.nil();
                root_1 = (EvaluationExpression)adaptor.becomeRoot(
                (EvaluationExpression)adaptor.create(EXPRESSION, "ElementInSetExpression")
                , root_1);

                adaptor.addChild(root_1, stream_elem.nextTree());

                adaptor.addChild(root_1,  not == null ? ElementInSetExpression.Quantor.EXISTS_IN : ElementInSetExpression.Quantor.EXISTS_NOT_IN);

                adaptor.addChild(root_1, stream_set.nextTree());

                adaptor.addChild(root_0, root_1);
                }

            }


            retval.tree = root_0;
            }

            }

            retval.stop = input.LT(-1);


            if ( state.backtracking==0 ) {

            retval.tree = (EvaluationExpression)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }

        catch (RecognitionException e) {
          throw e;
        }

        finally {
        	// do for sure before leaving
        }
        return retval;
    }
    // $ANTLR end "elementExpression"


    public static class comparisonExpression_return extends ParserRuleReturnScope {
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };


    // $ANTLR start "comparisonExpression"
    // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:177:1: comparisonExpression : e1= arithmeticExpression ( (s= '<=' |s= '>=' |s= '<' |s= '>' |s= '==' |s= '!=' ) e2= comparisonExpression )? -> { $s == null }? $e1 -> { $s.getText().equals(\"!=\") }? ^( EXPRESSION[\"ComparativeExpression\"] $e1 $e2) -> { $s.getText().equals(\"==\") }? ^( EXPRESSION[\"ComparativeExpression\"] $e1 $e2) -> ^( EXPRESSION[\"ComparativeExpression\"] $e1 $e2) ;
    public final MeteorParser.comparisonExpression_return comparisonExpression() throws RecognitionException {
        MeteorParser.comparisonExpression_return retval = new MeteorParser.comparisonExpression_return();
        retval.start = input.LT(1);


        EvaluationExpression root_0 = null;

        Token s=null;
        MeteorParser.arithmeticExpression_return e1 =null;

        MeteorParser.comparisonExpression_return e2 =null;


        EvaluationExpression s_tree=null;
        RewriteRuleTokenStream stream_56=new RewriteRuleTokenStream(adaptor,"token 56");
        RewriteRuleTokenStream stream_55=new RewriteRuleTokenStream(adaptor,"token 55");
        RewriteRuleTokenStream stream_51=new RewriteRuleTokenStream(adaptor,"token 51");
        RewriteRuleTokenStream stream_52=new RewriteRuleTokenStream(adaptor,"token 52");
        RewriteRuleTokenStream stream_54=new RewriteRuleTokenStream(adaptor,"token 54");
        RewriteRuleTokenStream stream_37=new RewriteRuleTokenStream(adaptor,"token 37");
        RewriteRuleSubtreeStream stream_arithmeticExpression=new RewriteRuleSubtreeStream(adaptor,"rule arithmeticExpression");
        RewriteRuleSubtreeStream stream_comparisonExpression=new RewriteRuleSubtreeStream(adaptor,"rule comparisonExpression");
        try {
            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:178:2: (e1= arithmeticExpression ( (s= '<=' |s= '>=' |s= '<' |s= '>' |s= '==' |s= '!=' ) e2= comparisonExpression )? -> { $s == null }? $e1 -> { $s.getText().equals(\"!=\") }? ^( EXPRESSION[\"ComparativeExpression\"] $e1 $e2) -> { $s.getText().equals(\"==\") }? ^( EXPRESSION[\"ComparativeExpression\"] $e1 $e2) -> ^( EXPRESSION[\"ComparativeExpression\"] $e1 $e2) )
            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:178:4: e1= arithmeticExpression ( (s= '<=' |s= '>=' |s= '<' |s= '>' |s= '==' |s= '!=' ) e2= comparisonExpression )?
            {
            pushFollow(FOLLOW_arithmeticExpression_in_comparisonExpression739);
            e1=arithmeticExpression();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_arithmeticExpression.add(e1.getTree());

            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:178:28: ( (s= '<=' |s= '>=' |s= '<' |s= '>' |s= '==' |s= '!=' ) e2= comparisonExpression )?
            int alt18=2;
            int LA18_0 = input.LA(1);

            if ( (LA18_0==37||(LA18_0 >= 51 && LA18_0 <= 52)||(LA18_0 >= 54 && LA18_0 <= 56)) ) {
                alt18=1;
            }
            switch (alt18) {
                case 1 :
                    // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:178:29: (s= '<=' |s= '>=' |s= '<' |s= '>' |s= '==' |s= '!=' ) e2= comparisonExpression
                    {
                    // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:178:29: (s= '<=' |s= '>=' |s= '<' |s= '>' |s= '==' |s= '!=' )
                    int alt17=6;
                    switch ( input.LA(1) ) {
                    case 52:
                        {
                        alt17=1;
                        }
                        break;
                    case 56:
                        {
                        alt17=2;
                        }
                        break;
                    case 51:
                        {
                        alt17=3;
                        }
                        break;
                    case 55:
                        {
                        alt17=4;
                        }
                        break;
                    case 54:
                        {
                        alt17=5;
                        }
                        break;
                    case 37:
                        {
                        alt17=6;
                        }
                        break;
                    default:
                        if (state.backtracking>0) {state.failed=true; return retval;}
                        NoViableAltException nvae =
                            new NoViableAltException("", 17, 0, input);

                        throw nvae;

                    }

                    switch (alt17) {
                        case 1 :
                            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:178:30: s= '<='
                            {
                            s=(Token)match(input,52,FOLLOW_52_in_comparisonExpression745); if (state.failed) return retval; 
                            if ( state.backtracking==0 ) stream_52.add(s);


                            }
                            break;
                        case 2 :
                            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:178:39: s= '>='
                            {
                            s=(Token)match(input,56,FOLLOW_56_in_comparisonExpression751); if (state.failed) return retval; 
                            if ( state.backtracking==0 ) stream_56.add(s);


                            }
                            break;
                        case 3 :
                            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:178:48: s= '<'
                            {
                            s=(Token)match(input,51,FOLLOW_51_in_comparisonExpression757); if (state.failed) return retval; 
                            if ( state.backtracking==0 ) stream_51.add(s);


                            }
                            break;
                        case 4 :
                            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:178:56: s= '>'
                            {
                            s=(Token)match(input,55,FOLLOW_55_in_comparisonExpression763); if (state.failed) return retval; 
                            if ( state.backtracking==0 ) stream_55.add(s);


                            }
                            break;
                        case 5 :
                            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:178:64: s= '=='
                            {
                            s=(Token)match(input,54,FOLLOW_54_in_comparisonExpression769); if (state.failed) return retval; 
                            if ( state.backtracking==0 ) stream_54.add(s);


                            }
                            break;
                        case 6 :
                            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:178:73: s= '!='
                            {
                            s=(Token)match(input,37,FOLLOW_37_in_comparisonExpression775); if (state.failed) return retval; 
                            if ( state.backtracking==0 ) stream_37.add(s);


                            }
                            break;

                    }


                    pushFollow(FOLLOW_comparisonExpression_in_comparisonExpression780);
                    e2=comparisonExpression();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_comparisonExpression.add(e2.getTree());

                    }
                    break;

            }


            // AST REWRITE
            // elements: e2, e1, e1, e1, e2, e1, e2
            // token labels: 
            // rule labels: retval, e1, e2
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            if ( state.backtracking==0 ) {

            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);
            RewriteRuleSubtreeStream stream_e1=new RewriteRuleSubtreeStream(adaptor,"rule e1",e1!=null?e1.tree:null);
            RewriteRuleSubtreeStream stream_e2=new RewriteRuleSubtreeStream(adaptor,"rule e2",e2!=null?e2.tree:null);

            root_0 = (EvaluationExpression)adaptor.nil();
            // 179:2: -> { $s == null }? $e1
            if ( s == null ) {
                adaptor.addChild(root_0, stream_e1.nextTree());

            }

            else // 180:3: -> { $s.getText().equals(\"!=\") }? ^( EXPRESSION[\"ComparativeExpression\"] $e1 $e2)
            if ( s.getText().equals("!=") ) {
                // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:180:38: ^( EXPRESSION[\"ComparativeExpression\"] $e1 $e2)
                {
                EvaluationExpression root_1 = (EvaluationExpression)adaptor.nil();
                root_1 = (EvaluationExpression)adaptor.becomeRoot(
                (EvaluationExpression)adaptor.create(EXPRESSION, "ComparativeExpression")
                , root_1);

                adaptor.addChild(root_1, stream_e1.nextTree());

                adaptor.addChild(root_1, ComparativeExpression.BinaryOperator.NOT_EQUAL);

                adaptor.addChild(root_1, stream_e2.nextTree());

                adaptor.addChild(root_0, root_1);
                }

            }

            else // 181:3: -> { $s.getText().equals(\"==\") }? ^( EXPRESSION[\"ComparativeExpression\"] $e1 $e2)
            if ( s.getText().equals("==") ) {
                // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:181:38: ^( EXPRESSION[\"ComparativeExpression\"] $e1 $e2)
                {
                EvaluationExpression root_1 = (EvaluationExpression)adaptor.nil();
                root_1 = (EvaluationExpression)adaptor.becomeRoot(
                (EvaluationExpression)adaptor.create(EXPRESSION, "ComparativeExpression")
                , root_1);

                adaptor.addChild(root_1, stream_e1.nextTree());

                adaptor.addChild(root_1, ComparativeExpression.BinaryOperator.EQUAL);

                adaptor.addChild(root_1, stream_e2.nextTree());

                adaptor.addChild(root_0, root_1);
                }

            }

            else // 182:2: -> ^( EXPRESSION[\"ComparativeExpression\"] $e1 $e2)
            {
                // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:182:6: ^( EXPRESSION[\"ComparativeExpression\"] $e1 $e2)
                {
                EvaluationExpression root_1 = (EvaluationExpression)adaptor.nil();
                root_1 = (EvaluationExpression)adaptor.becomeRoot(
                (EvaluationExpression)adaptor.create(EXPRESSION, "ComparativeExpression")
                , root_1);

                adaptor.addChild(root_1, stream_e1.nextTree());

                adaptor.addChild(root_1, ComparativeExpression.BinaryOperator.valueOfSymbol((s!=null?s.getText():null)));

                adaptor.addChild(root_1, stream_e2.nextTree());

                adaptor.addChild(root_0, root_1);
                }

            }


            retval.tree = root_0;
            }

            }

            retval.stop = input.LT(-1);


            if ( state.backtracking==0 ) {

            retval.tree = (EvaluationExpression)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }

        catch (RecognitionException e) {
          throw e;
        }

        finally {
        	// do for sure before leaving
        }
        return retval;
    }
    // $ANTLR end "comparisonExpression"


    public static class arithmeticExpression_return extends ParserRuleReturnScope {
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };


    // $ANTLR start "arithmeticExpression"
    // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:184:1: arithmeticExpression : e1= multiplicationExpression ( (s= '+' |s= '-' ) e2= arithmeticExpression )? -> { s != null }? ^( EXPRESSION[\"ArithmeticExpression\"] $e1 $e2) -> $e1;
    public final MeteorParser.arithmeticExpression_return arithmeticExpression() throws RecognitionException {
        MeteorParser.arithmeticExpression_return retval = new MeteorParser.arithmeticExpression_return();
        retval.start = input.LT(1);


        EvaluationExpression root_0 = null;

        Token s=null;
        MeteorParser.multiplicationExpression_return e1 =null;

        MeteorParser.arithmeticExpression_return e2 =null;


        EvaluationExpression s_tree=null;
        RewriteRuleTokenStream stream_43=new RewriteRuleTokenStream(adaptor,"token 43");
        RewriteRuleTokenStream stream_46=new RewriteRuleTokenStream(adaptor,"token 46");
        RewriteRuleSubtreeStream stream_arithmeticExpression=new RewriteRuleSubtreeStream(adaptor,"rule arithmeticExpression");
        RewriteRuleSubtreeStream stream_multiplicationExpression=new RewriteRuleSubtreeStream(adaptor,"rule multiplicationExpression");
        try {
            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:185:2: (e1= multiplicationExpression ( (s= '+' |s= '-' ) e2= arithmeticExpression )? -> { s != null }? ^( EXPRESSION[\"ArithmeticExpression\"] $e1 $e2) -> $e1)
            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:185:4: e1= multiplicationExpression ( (s= '+' |s= '-' ) e2= arithmeticExpression )?
            {
            pushFollow(FOLLOW_multiplicationExpression_in_arithmeticExpression860);
            e1=multiplicationExpression();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_multiplicationExpression.add(e1.getTree());

            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:185:32: ( (s= '+' |s= '-' ) e2= arithmeticExpression )?
            int alt20=2;
            int LA20_0 = input.LA(1);

            if ( (LA20_0==43||LA20_0==46) ) {
                alt20=1;
            }
            switch (alt20) {
                case 1 :
                    // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:185:33: (s= '+' |s= '-' ) e2= arithmeticExpression
                    {
                    // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:185:33: (s= '+' |s= '-' )
                    int alt19=2;
                    int LA19_0 = input.LA(1);

                    if ( (LA19_0==43) ) {
                        alt19=1;
                    }
                    else if ( (LA19_0==46) ) {
                        alt19=2;
                    }
                    else {
                        if (state.backtracking>0) {state.failed=true; return retval;}
                        NoViableAltException nvae =
                            new NoViableAltException("", 19, 0, input);

                        throw nvae;

                    }
                    switch (alt19) {
                        case 1 :
                            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:185:34: s= '+'
                            {
                            s=(Token)match(input,43,FOLLOW_43_in_arithmeticExpression866); if (state.failed) return retval; 
                            if ( state.backtracking==0 ) stream_43.add(s);


                            }
                            break;
                        case 2 :
                            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:185:42: s= '-'
                            {
                            s=(Token)match(input,46,FOLLOW_46_in_arithmeticExpression872); if (state.failed) return retval; 
                            if ( state.backtracking==0 ) stream_46.add(s);


                            }
                            break;

                    }


                    pushFollow(FOLLOW_arithmeticExpression_in_arithmeticExpression877);
                    e2=arithmeticExpression();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_arithmeticExpression.add(e2.getTree());

                    }
                    break;

            }


            // AST REWRITE
            // elements: e1, e2, e1
            // token labels: 
            // rule labels: retval, e1, e2
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            if ( state.backtracking==0 ) {

            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);
            RewriteRuleSubtreeStream stream_e1=new RewriteRuleSubtreeStream(adaptor,"rule e1",e1!=null?e1.tree:null);
            RewriteRuleSubtreeStream stream_e2=new RewriteRuleSubtreeStream(adaptor,"rule e2",e2!=null?e2.tree:null);

            root_0 = (EvaluationExpression)adaptor.nil();
            // 186:2: -> { s != null }? ^( EXPRESSION[\"ArithmeticExpression\"] $e1 $e2)
            if ( s != null ) {
                // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:186:21: ^( EXPRESSION[\"ArithmeticExpression\"] $e1 $e2)
                {
                EvaluationExpression root_1 = (EvaluationExpression)adaptor.nil();
                root_1 = (EvaluationExpression)adaptor.becomeRoot(
                (EvaluationExpression)adaptor.create(EXPRESSION, "ArithmeticExpression")
                , root_1);

                adaptor.addChild(root_1, stream_e1.nextTree());

                adaptor.addChild(root_1,  s.getText().equals("+") ? ArithmeticExpression.ArithmeticOperator.ADDITION : ArithmeticExpression.ArithmeticOperator.SUBTRACTION);

                adaptor.addChild(root_1, stream_e2.nextTree());

                adaptor.addChild(root_0, root_1);
                }

            }

            else // 188:2: -> $e1
            {
                adaptor.addChild(root_0, stream_e1.nextTree());

            }


            retval.tree = root_0;
            }

            }

            retval.stop = input.LT(-1);


            if ( state.backtracking==0 ) {

            retval.tree = (EvaluationExpression)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }

        catch (RecognitionException e) {
          throw e;
        }

        finally {
        	// do for sure before leaving
        }
        return retval;
    }
    // $ANTLR end "arithmeticExpression"


    public static class multiplicationExpression_return extends ParserRuleReturnScope {
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };


    // $ANTLR start "multiplicationExpression"
    // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:190:1: multiplicationExpression : e1= preincrementExpression ( (s= '*' |s= SLASH ) e2= preincrementExpression )? -> { s != null }? ^( EXPRESSION[\"ArithmeticExpression\"] $e1 $e2) -> $e1;
    public final MeteorParser.multiplicationExpression_return multiplicationExpression() throws RecognitionException {
        MeteorParser.multiplicationExpression_return retval = new MeteorParser.multiplicationExpression_return();
        retval.start = input.LT(1);


        EvaluationExpression root_0 = null;

        Token s=null;
        MeteorParser.preincrementExpression_return e1 =null;

        MeteorParser.preincrementExpression_return e2 =null;


        EvaluationExpression s_tree=null;
        RewriteRuleTokenStream stream_STAR=new RewriteRuleTokenStream(adaptor,"token STAR");
        RewriteRuleTokenStream stream_SLASH=new RewriteRuleTokenStream(adaptor,"token SLASH");
        RewriteRuleSubtreeStream stream_preincrementExpression=new RewriteRuleSubtreeStream(adaptor,"rule preincrementExpression");
        try {
            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:191:2: (e1= preincrementExpression ( (s= '*' |s= SLASH ) e2= preincrementExpression )? -> { s != null }? ^( EXPRESSION[\"ArithmeticExpression\"] $e1 $e2) -> $e1)
            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:191:4: e1= preincrementExpression ( (s= '*' |s= SLASH ) e2= preincrementExpression )?
            {
            pushFollow(FOLLOW_preincrementExpression_in_multiplicationExpression920);
            e1=preincrementExpression();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_preincrementExpression.add(e1.getTree());

            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:191:30: ( (s= '*' |s= SLASH ) e2= preincrementExpression )?
            int alt22=2;
            int LA22_0 = input.LA(1);

            if ( ((LA22_0 >= SLASH && LA22_0 <= STAR)) ) {
                alt22=1;
            }
            switch (alt22) {
                case 1 :
                    // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:191:31: (s= '*' |s= SLASH ) e2= preincrementExpression
                    {
                    // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:191:31: (s= '*' |s= SLASH )
                    int alt21=2;
                    int LA21_0 = input.LA(1);

                    if ( (LA21_0==STAR) ) {
                        alt21=1;
                    }
                    else if ( (LA21_0==SLASH) ) {
                        alt21=2;
                    }
                    else {
                        if (state.backtracking>0) {state.failed=true; return retval;}
                        NoViableAltException nvae =
                            new NoViableAltException("", 21, 0, input);

                        throw nvae;

                    }
                    switch (alt21) {
                        case 1 :
                            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:191:32: s= '*'
                            {
                            s=(Token)match(input,STAR,FOLLOW_STAR_in_multiplicationExpression926); if (state.failed) return retval; 
                            if ( state.backtracking==0 ) stream_STAR.add(s);


                            }
                            break;
                        case 2 :
                            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:191:40: s= SLASH
                            {
                            s=(Token)match(input,SLASH,FOLLOW_SLASH_in_multiplicationExpression932); if (state.failed) return retval; 
                            if ( state.backtracking==0 ) stream_SLASH.add(s);


                            }
                            break;

                    }


                    pushFollow(FOLLOW_preincrementExpression_in_multiplicationExpression937);
                    e2=preincrementExpression();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_preincrementExpression.add(e2.getTree());

                    }
                    break;

            }


            // AST REWRITE
            // elements: e1, e1, e2
            // token labels: 
            // rule labels: retval, e1, e2
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            if ( state.backtracking==0 ) {

            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);
            RewriteRuleSubtreeStream stream_e1=new RewriteRuleSubtreeStream(adaptor,"rule e1",e1!=null?e1.tree:null);
            RewriteRuleSubtreeStream stream_e2=new RewriteRuleSubtreeStream(adaptor,"rule e2",e2!=null?e2.tree:null);

            root_0 = (EvaluationExpression)adaptor.nil();
            // 192:2: -> { s != null }? ^( EXPRESSION[\"ArithmeticExpression\"] $e1 $e2)
            if ( s != null ) {
                // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:192:21: ^( EXPRESSION[\"ArithmeticExpression\"] $e1 $e2)
                {
                EvaluationExpression root_1 = (EvaluationExpression)adaptor.nil();
                root_1 = (EvaluationExpression)adaptor.becomeRoot(
                (EvaluationExpression)adaptor.create(EXPRESSION, "ArithmeticExpression")
                , root_1);

                adaptor.addChild(root_1, stream_e1.nextTree());

                adaptor.addChild(root_1,  s.getText().equals("*") ? ArithmeticExpression.ArithmeticOperator.MULTIPLICATION : ArithmeticExpression.ArithmeticOperator.DIVISION);

                adaptor.addChild(root_1, stream_e2.nextTree());

                adaptor.addChild(root_0, root_1);
                }

            }

            else // 194:2: -> $e1
            {
                adaptor.addChild(root_0, stream_e1.nextTree());

            }


            retval.tree = root_0;
            }

            }

            retval.stop = input.LT(-1);


            if ( state.backtracking==0 ) {

            retval.tree = (EvaluationExpression)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }

        catch (RecognitionException e) {
          throw e;
        }

        finally {
        	// do for sure before leaving
        }
        return retval;
    }
    // $ANTLR end "multiplicationExpression"


    public static class preincrementExpression_return extends ParserRuleReturnScope {
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };


    // $ANTLR start "preincrementExpression"
    // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:196:1: preincrementExpression : ( '++' preincrementExpression | '--' preincrementExpression | unaryExpression );
    public final MeteorParser.preincrementExpression_return preincrementExpression() throws RecognitionException {
        MeteorParser.preincrementExpression_return retval = new MeteorParser.preincrementExpression_return();
        retval.start = input.LT(1);


        EvaluationExpression root_0 = null;

        Token string_literal38=null;
        Token string_literal40=null;
        MeteorParser.preincrementExpression_return preincrementExpression39 =null;

        MeteorParser.preincrementExpression_return preincrementExpression41 =null;

        MeteorParser.unaryExpression_return unaryExpression42 =null;


        EvaluationExpression string_literal38_tree=null;
        EvaluationExpression string_literal40_tree=null;

        try {
            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:197:2: ( '++' preincrementExpression | '--' preincrementExpression | unaryExpression )
            int alt23=3;
            switch ( input.LA(1) ) {
            case 44:
                {
                alt23=1;
                }
                break;
            case 47:
                {
                alt23=2;
                }
                break;
            case DECIMAL:
            case FN:
            case ID:
            case INTEGER:
            case STRING:
            case UINT:
            case VAR:
            case 36:
            case 40:
            case 41:
            case 59:
            case 61:
            case 62:
            case 64:
            case 67:
            case 70:
                {
                alt23=3;
                }
                break;
            default:
                if (state.backtracking>0) {state.failed=true; return retval;}
                NoViableAltException nvae =
                    new NoViableAltException("", 23, 0, input);

                throw nvae;

            }

            switch (alt23) {
                case 1 :
                    // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:197:4: '++' preincrementExpression
                    {
                    root_0 = (EvaluationExpression)adaptor.nil();


                    string_literal38=(Token)match(input,44,FOLLOW_44_in_preincrementExpression978); if (state.failed) return retval;
                    if ( state.backtracking==0 ) {
                    string_literal38_tree = 
                    (EvaluationExpression)adaptor.create(string_literal38)
                    ;
                    adaptor.addChild(root_0, string_literal38_tree);
                    }

                    pushFollow(FOLLOW_preincrementExpression_in_preincrementExpression980);
                    preincrementExpression39=preincrementExpression();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, preincrementExpression39.getTree());

                    }
                    break;
                case 2 :
                    // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:198:4: '--' preincrementExpression
                    {
                    root_0 = (EvaluationExpression)adaptor.nil();


                    string_literal40=(Token)match(input,47,FOLLOW_47_in_preincrementExpression985); if (state.failed) return retval;
                    if ( state.backtracking==0 ) {
                    string_literal40_tree = 
                    (EvaluationExpression)adaptor.create(string_literal40)
                    ;
                    adaptor.addChild(root_0, string_literal40_tree);
                    }

                    pushFollow(FOLLOW_preincrementExpression_in_preincrementExpression987);
                    preincrementExpression41=preincrementExpression();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, preincrementExpression41.getTree());

                    }
                    break;
                case 3 :
                    // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:199:4: unaryExpression
                    {
                    root_0 = (EvaluationExpression)adaptor.nil();


                    pushFollow(FOLLOW_unaryExpression_in_preincrementExpression992);
                    unaryExpression42=unaryExpression();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, unaryExpression42.getTree());

                    }
                    break;

            }
            retval.stop = input.LT(-1);


            if ( state.backtracking==0 ) {

            retval.tree = (EvaluationExpression)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }

        catch (RecognitionException e) {
          throw e;
        }

        finally {
        	// do for sure before leaving
        }
        return retval;
    }
    // $ANTLR end "preincrementExpression"


    public static class unaryExpression_return extends ParserRuleReturnScope {
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };


    // $ANTLR start "unaryExpression"
    // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:201:1: unaryExpression : ( '!' | '~' )? castExpression ;
    public final MeteorParser.unaryExpression_return unaryExpression() throws RecognitionException {
        MeteorParser.unaryExpression_return retval = new MeteorParser.unaryExpression_return();
        retval.start = input.LT(1);


        EvaluationExpression root_0 = null;

        Token set43=null;
        MeteorParser.castExpression_return castExpression44 =null;


        EvaluationExpression set43_tree=null;

        try {
            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:202:2: ( ( '!' | '~' )? castExpression )
            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:202:4: ( '!' | '~' )? castExpression
            {
            root_0 = (EvaluationExpression)adaptor.nil();


            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:202:4: ( '!' | '~' )?
            int alt24=2;
            int LA24_0 = input.LA(1);

            if ( (LA24_0==36||LA24_0==70) ) {
                alt24=1;
            }
            switch (alt24) {
                case 1 :
                    // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:
                    {
                    set43=(Token)input.LT(1);

                    if ( input.LA(1)==36||input.LA(1)==70 ) {
                        input.consume();
                        if ( state.backtracking==0 ) adaptor.addChild(root_0, 
                        (EvaluationExpression)adaptor.create(set43)
                        );
                        state.errorRecovery=false;
                        state.failed=false;
                    }
                    else {
                        if (state.backtracking>0) {state.failed=true; return retval;}
                        MismatchedSetException mse = new MismatchedSetException(null,input);
                        throw mse;
                    }


                    }
                    break;

            }


            pushFollow(FOLLOW_castExpression_in_unaryExpression1011);
            castExpression44=castExpression();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) adaptor.addChild(root_0, castExpression44.getTree());

            }

            retval.stop = input.LT(-1);


            if ( state.backtracking==0 ) {

            retval.tree = (EvaluationExpression)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }

        catch (RecognitionException e) {
          throw e;
        }

        finally {
        	// do for sure before leaving
        }
        return retval;
    }
    // $ANTLR end "unaryExpression"


    public static class castExpression_return extends ParserRuleReturnScope {
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };


    // $ANTLR start "castExpression"
    // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:208:1: castExpression : ( ( '(' ID ')' )=> '(' type= ID ')' expr= generalPathExpression ->|expr= generalPathExpression ({...}? ID type= ID )? ->);
    public final MeteorParser.castExpression_return castExpression() throws RecognitionException {
        MeteorParser.castExpression_return retval = new MeteorParser.castExpression_return();
        retval.start = input.LT(1);


        EvaluationExpression root_0 = null;

        Token type=null;
        Token char_literal45=null;
        Token char_literal46=null;
        Token ID47=null;
        MeteorParser.generalPathExpression_return expr =null;


        EvaluationExpression type_tree=null;
        EvaluationExpression char_literal45_tree=null;
        EvaluationExpression char_literal46_tree=null;
        EvaluationExpression ID47_tree=null;
        RewriteRuleTokenStream stream_42=new RewriteRuleTokenStream(adaptor,"token 42");
        RewriteRuleTokenStream stream_41=new RewriteRuleTokenStream(adaptor,"token 41");
        RewriteRuleTokenStream stream_ID=new RewriteRuleTokenStream(adaptor,"token ID");
        RewriteRuleSubtreeStream stream_generalPathExpression=new RewriteRuleSubtreeStream(adaptor,"rule generalPathExpression");
        try {
            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:209:2: ( ( '(' ID ')' )=> '(' type= ID ')' expr= generalPathExpression ->|expr= generalPathExpression ({...}? ID type= ID )? ->)
            int alt26=2;
            int LA26_0 = input.LA(1);

            if ( (LA26_0==41) ) {
                int LA26_1 = input.LA(2);

                if ( (synpred7_Meteor()) ) {
                    alt26=1;
                }
                else if ( (true) ) {
                    alt26=2;
                }
                else {
                    if (state.backtracking>0) {state.failed=true; return retval;}
                    NoViableAltException nvae =
                        new NoViableAltException("", 26, 1, input);

                    throw nvae;

                }
            }
            else if ( (LA26_0==DECIMAL||LA26_0==FN||LA26_0==ID||LA26_0==INTEGER||(LA26_0 >= STRING && LA26_0 <= UINT)||LA26_0==VAR||LA26_0==40||LA26_0==59||(LA26_0 >= 61 && LA26_0 <= 62)||LA26_0==64||LA26_0==67) ) {
                alt26=2;
            }
            else {
                if (state.backtracking>0) {state.failed=true; return retval;}
                NoViableAltException nvae =
                    new NoViableAltException("", 26, 0, input);

                throw nvae;

            }
            switch (alt26) {
                case 1 :
                    // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:209:4: ( '(' ID ')' )=> '(' type= ID ')' expr= generalPathExpression
                    {
                    char_literal45=(Token)match(input,41,FOLLOW_41_in_castExpression1031); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_41.add(char_literal45);


                    type=(Token)match(input,ID,FOLLOW_ID_in_castExpression1035); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_ID.add(type);


                    char_literal46=(Token)match(input,42,FOLLOW_42_in_castExpression1037); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_42.add(char_literal46);


                    pushFollow(FOLLOW_generalPathExpression_in_castExpression1041);
                    expr=generalPathExpression();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_generalPathExpression.add(expr.getTree());

                    // AST REWRITE
                    // elements: 
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    if ( state.backtracking==0 ) {

                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (EvaluationExpression)adaptor.nil();
                    // 210:3: ->
                    {
                        adaptor.addChild(root_0,  coerce((type!=null?type.getText():null), (expr!=null?((EvaluationExpression)expr.tree):null)) );

                    }


                    retval.tree = root_0;
                    }

                    }
                    break;
                case 2 :
                    // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:211:4: expr= generalPathExpression ({...}? ID type= ID )?
                    {
                    pushFollow(FOLLOW_generalPathExpression_in_castExpression1054);
                    expr=generalPathExpression();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_generalPathExpression.add(expr.getTree());

                    // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:211:31: ({...}? ID type= ID )?
                    int alt25=2;
                    int LA25_0 = input.LA(1);

                    if ( (LA25_0==ID) ) {
                        int LA25_1 = input.LA(2);

                        if ( (LA25_1==ID) ) {
                            int LA25_5 = input.LA(3);

                            if ( ((input.LT(1).getText().equals("as"))) ) {
                                alt25=1;
                            }
                        }
                    }
                    switch (alt25) {
                        case 1 :
                            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:211:32: {...}? ID type= ID
                            {
                            if ( !((input.LT(1).getText().equals("as"))) ) {
                                if (state.backtracking>0) {state.failed=true; return retval;}
                                throw new FailedPredicateException(input, "castExpression", "input.LT(1).getText().equals(\"as\")");
                            }

                            ID47=(Token)match(input,ID,FOLLOW_ID_in_castExpression1059); if (state.failed) return retval; 
                            if ( state.backtracking==0 ) stream_ID.add(ID47);


                            type=(Token)match(input,ID,FOLLOW_ID_in_castExpression1063); if (state.failed) return retval; 
                            if ( state.backtracking==0 ) stream_ID.add(type);


                            }
                            break;

                    }


                    // AST REWRITE
                    // elements: 
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    if ( state.backtracking==0 ) {

                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (EvaluationExpression)adaptor.nil();
                    // 212:2: ->
                    {
                        adaptor.addChild(root_0,  type == null ? (expr!=null?((EvaluationExpression)expr.tree):null) : coerce((type!=null?type.getText():null), (expr!=null?((EvaluationExpression)expr.tree):null)));

                    }


                    retval.tree = root_0;
                    }

                    }
                    break;

            }
            retval.stop = input.LT(-1);


            if ( state.backtracking==0 ) {

            retval.tree = (EvaluationExpression)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }

        catch (RecognitionException e) {
          throw e;
        }

        finally {
        	// do for sure before leaving
        }
        return retval;
    }
    // $ANTLR end "castExpression"


    public static class generalPathExpression_return extends ParserRuleReturnScope {
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };


    // $ANTLR start "generalPathExpression"
    // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:214:1: generalPathExpression : value= valueExpression ( ( pathExpression[EvaluationExpression.VALUE] )=>path= pathExpression[$value.tree] -> $path| -> $value) ;
    public final MeteorParser.generalPathExpression_return generalPathExpression() throws RecognitionException {
        MeteorParser.generalPathExpression_return retval = new MeteorParser.generalPathExpression_return();
        retval.start = input.LT(1);


        EvaluationExpression root_0 = null;

        MeteorParser.valueExpression_return value =null;

        MeteorParser.pathExpression_return path =null;


        RewriteRuleSubtreeStream stream_valueExpression=new RewriteRuleSubtreeStream(adaptor,"rule valueExpression");
        RewriteRuleSubtreeStream stream_pathExpression=new RewriteRuleSubtreeStream(adaptor,"rule pathExpression");
        try {
            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:215:2: (value= valueExpression ( ( pathExpression[EvaluationExpression.VALUE] )=>path= pathExpression[$value.tree] -> $path| -> $value) )
            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:215:4: value= valueExpression ( ( pathExpression[EvaluationExpression.VALUE] )=>path= pathExpression[$value.tree] -> $path| -> $value)
            {
            pushFollow(FOLLOW_valueExpression_in_generalPathExpression1082);
            value=valueExpression();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_valueExpression.add(value.getTree());

            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:216:4: ( ( pathExpression[EvaluationExpression.VALUE] )=>path= pathExpression[$value.tree] -> $path| -> $value)
            int alt27=2;
            int LA27_0 = input.LA(1);

            if ( (LA27_0==58) && (synpred8_Meteor())) {
                alt27=1;
            }
            else if ( (LA27_0==48) && (synpred8_Meteor())) {
                alt27=1;
            }
            else if ( (LA27_0==59) && (synpred8_Meteor())) {
                alt27=1;
            }
            else if ( (LA27_0==EOF||LA27_0==AND||(LA27_0 >= ID && LA27_0 <= IN)||LA27_0==NOT||LA27_0==OR||(LA27_0 >= SLASH && LA27_0 <= STAR)||LA27_0==37||LA27_0==39||(LA27_0 >= 42 && LA27_0 <= 43)||(LA27_0 >= 45 && LA27_0 <= 46)||(LA27_0 >= 49 && LA27_0 <= 52)||(LA27_0 >= 54 && LA27_0 <= 57)||LA27_0==60||(LA27_0 >= 68 && LA27_0 <= 69)) ) {
                alt27=2;
            }
            else {
                if (state.backtracking>0) {state.failed=true; return retval;}
                NoViableAltException nvae =
                    new NoViableAltException("", 27, 0, input);

                throw nvae;

            }
            switch (alt27) {
                case 1 :
                    // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:216:5: ( pathExpression[EvaluationExpression.VALUE] )=>path= pathExpression[$value.tree]
                    {
                    pushFollow(FOLLOW_pathExpression_in_generalPathExpression1097);
                    path=pathExpression((value!=null?((EvaluationExpression)value.tree):null));

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_pathExpression.add(path.getTree());

                    // AST REWRITE
                    // elements: path
                    // token labels: 
                    // rule labels: retval, path
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    if ( state.backtracking==0 ) {

                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);
                    RewriteRuleSubtreeStream stream_path=new RewriteRuleSubtreeStream(adaptor,"rule path",path!=null?path.tree:null);

                    root_0 = (EvaluationExpression)adaptor.nil();
                    // 216:85: -> $path
                    {
                        adaptor.addChild(root_0, stream_path.nextTree());

                    }


                    retval.tree = root_0;
                    }

                    }
                    break;
                case 2 :
                    // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:217:7: 
                    {
                    // AST REWRITE
                    // elements: value
                    // token labels: 
                    // rule labels: retval, value
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    if ( state.backtracking==0 ) {

                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);
                    RewriteRuleSubtreeStream stream_value=new RewriteRuleSubtreeStream(adaptor,"rule value",value!=null?value.tree:null);

                    root_0 = (EvaluationExpression)adaptor.nil();
                    // 217:7: -> $value
                    {
                        adaptor.addChild(root_0, stream_value.nextTree());

                    }


                    retval.tree = root_0;
                    }

                    }
                    break;

            }


            }

            retval.stop = input.LT(-1);


            if ( state.backtracking==0 ) {

            retval.tree = (EvaluationExpression)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }

        catch (RecognitionException e) {
          throw e;
        }

        finally {
        	// do for sure before leaving
        }
        return retval;
    }
    // $ANTLR end "generalPathExpression"


    public static class contextAwarePathExpression_return extends ParserRuleReturnScope {
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };


    // $ANTLR start "contextAwarePathExpression"
    // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:219:1: contextAwarePathExpression[EvaluationExpression context] : pathExpression[context] ;
    public final MeteorParser.contextAwarePathExpression_return contextAwarePathExpression(EvaluationExpression context) throws RecognitionException {
        MeteorParser.contextAwarePathExpression_return retval = new MeteorParser.contextAwarePathExpression_return();
        retval.start = input.LT(1);


        EvaluationExpression root_0 = null;

        MeteorParser.pathExpression_return pathExpression48 =null;



        try {
            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:220:3: ( pathExpression[context] )
            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:220:5: pathExpression[context]
            {
            root_0 = (EvaluationExpression)adaptor.nil();


            pushFollow(FOLLOW_pathExpression_in_contextAwarePathExpression1126);
            pathExpression48=pathExpression(context);

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) adaptor.addChild(root_0, pathExpression48.getTree());

            }

            retval.stop = input.LT(-1);


            if ( state.backtracking==0 ) {

            retval.tree = (EvaluationExpression)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }

        catch (RecognitionException e) {
          throw e;
        }

        finally {
        	// do for sure before leaving
        }
        return retval;
    }
    // $ANTLR end "contextAwarePathExpression"


    public static class pathExpression_return extends ParserRuleReturnScope {
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };


    // $ANTLR start "pathExpression"
    // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:222:1: pathExpression[EvaluationExpression inExp] : ( ( '?.' ID '(' )=> '?.' call= methodCall[inExp] ( ( pathSegment )=>path= pathExpression[new TernaryExpression(new NotNullOrMissingBooleanExpression().withInputExpression(inExp), $call.tree)] -> $path| -> ^( EXPRESSION[\"TernaryExpression\"] $call) ) | ( '.' ID '(' )=> '.' call= methodCall[inExp] ( ( pathSegment )=>path= pathExpression[$call.tree] -> $path| -> $call) |seg= pathSegment ( ( pathSegment )=>path= pathExpression[$seg.tree] -> $path| -> $seg) );
    public final MeteorParser.pathExpression_return pathExpression(EvaluationExpression inExp) throws RecognitionException {
        MeteorParser.pathExpression_return retval = new MeteorParser.pathExpression_return();
        retval.start = input.LT(1);


        EvaluationExpression root_0 = null;

        Token string_literal49=null;
        Token char_literal50=null;
        MeteorParser.methodCall_return call =null;

        MeteorParser.pathExpression_return path =null;

        MeteorParser.pathSegment_return seg =null;


        EvaluationExpression string_literal49_tree=null;
        EvaluationExpression char_literal50_tree=null;
        RewriteRuleTokenStream stream_48=new RewriteRuleTokenStream(adaptor,"token 48");
        RewriteRuleTokenStream stream_58=new RewriteRuleTokenStream(adaptor,"token 58");
        RewriteRuleSubtreeStream stream_pathSegment=new RewriteRuleSubtreeStream(adaptor,"rule pathSegment");
        RewriteRuleSubtreeStream stream_methodCall=new RewriteRuleSubtreeStream(adaptor,"rule methodCall");
        RewriteRuleSubtreeStream stream_pathExpression=new RewriteRuleSubtreeStream(adaptor,"rule pathExpression");
        try {
            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:223:3: ( ( '?.' ID '(' )=> '?.' call= methodCall[inExp] ( ( pathSegment )=>path= pathExpression[new TernaryExpression(new NotNullOrMissingBooleanExpression().withInputExpression(inExp), $call.tree)] -> $path| -> ^( EXPRESSION[\"TernaryExpression\"] $call) ) | ( '.' ID '(' )=> '.' call= methodCall[inExp] ( ( pathSegment )=>path= pathExpression[$call.tree] -> $path| -> $call) |seg= pathSegment ( ( pathSegment )=>path= pathExpression[$seg.tree] -> $path| -> $seg) )
            int alt31=3;
            switch ( input.LA(1) ) {
            case 58:
                {
                int LA31_1 = input.LA(2);

                if ( (synpred9_Meteor()) ) {
                    alt31=1;
                }
                else if ( (true) ) {
                    alt31=3;
                }
                else {
                    if (state.backtracking>0) {state.failed=true; return retval;}
                    NoViableAltException nvae =
                        new NoViableAltException("", 31, 1, input);

                    throw nvae;

                }
                }
                break;
            case 48:
                {
                int LA31_2 = input.LA(2);

                if ( (synpred11_Meteor()) ) {
                    alt31=2;
                }
                else if ( (true) ) {
                    alt31=3;
                }
                else {
                    if (state.backtracking>0) {state.failed=true; return retval;}
                    NoViableAltException nvae =
                        new NoViableAltException("", 31, 2, input);

                    throw nvae;

                }
                }
                break;
            case 59:
                {
                alt31=3;
                }
                break;
            default:
                if (state.backtracking>0) {state.failed=true; return retval;}
                NoViableAltException nvae =
                    new NoViableAltException("", 31, 0, input);

                throw nvae;

            }

            switch (alt31) {
                case 1 :
                    // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:224:5: ( '?.' ID '(' )=> '?.' call= methodCall[inExp] ( ( pathSegment )=>path= pathExpression[new TernaryExpression(new NotNullOrMissingBooleanExpression().withInputExpression(inExp), $call.tree)] -> $path| -> ^( EXPRESSION[\"TernaryExpression\"] $call) )
                    {
                    string_literal49=(Token)match(input,58,FOLLOW_58_in_pathExpression1154); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_58.add(string_literal49);


                    pushFollow(FOLLOW_methodCall_in_pathExpression1158);
                    call=methodCall(inExp);

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_methodCall.add(call.getTree());

                    // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:225:7: ( ( pathSegment )=>path= pathExpression[new TernaryExpression(new NotNullOrMissingBooleanExpression().withInputExpression(inExp), $call.tree)] -> $path| -> ^( EXPRESSION[\"TernaryExpression\"] $call) )
                    int alt28=2;
                    int LA28_0 = input.LA(1);

                    if ( (LA28_0==58) && (synpred10_Meteor())) {
                        alt28=1;
                    }
                    else if ( (LA28_0==48) && (synpred10_Meteor())) {
                        alt28=1;
                    }
                    else if ( (LA28_0==59) && (synpred10_Meteor())) {
                        alt28=1;
                    }
                    else if ( (LA28_0==EOF||LA28_0==AND||(LA28_0 >= ID && LA28_0 <= IN)||LA28_0==NOT||LA28_0==OR||(LA28_0 >= SLASH && LA28_0 <= STAR)||LA28_0==37||LA28_0==39||(LA28_0 >= 42 && LA28_0 <= 43)||(LA28_0 >= 45 && LA28_0 <= 46)||(LA28_0 >= 49 && LA28_0 <= 52)||(LA28_0 >= 54 && LA28_0 <= 57)||LA28_0==60||(LA28_0 >= 68 && LA28_0 <= 69)) ) {
                        alt28=2;
                    }
                    else {
                        if (state.backtracking>0) {state.failed=true; return retval;}
                        NoViableAltException nvae =
                            new NoViableAltException("", 28, 0, input);

                        throw nvae;

                    }
                    switch (alt28) {
                        case 1 :
                            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:225:8: ( pathSegment )=>path= pathExpression[new TernaryExpression(new NotNullOrMissingBooleanExpression().withInputExpression(inExp), $call.tree)]
                            {
                            pushFollow(FOLLOW_pathExpression_in_pathExpression1175);
                            path=pathExpression(new TernaryExpression(new NotNullOrMissingBooleanExpression().withInputExpression(inExp), (call!=null?((EvaluationExpression)call.tree):null)));

                            state._fsp--;
                            if (state.failed) return retval;
                            if ( state.backtracking==0 ) stream_pathExpression.add(path.getTree());

                            // AST REWRITE
                            // elements: path
                            // token labels: 
                            // rule labels: retval, path
                            // token list labels: 
                            // rule list labels: 
                            // wildcard labels: 
                            if ( state.backtracking==0 ) {

                            retval.tree = root_0;
                            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);
                            RewriteRuleSubtreeStream stream_path=new RewriteRuleSubtreeStream(adaptor,"rule path",path!=null?path.tree:null);

                            root_0 = (EvaluationExpression)adaptor.nil();
                            // 225:146: -> $path
                            {
                                adaptor.addChild(root_0, stream_path.nextTree());

                            }


                            retval.tree = root_0;
                            }

                            }
                            break;
                        case 2 :
                            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:226:8: 
                            {
                            // AST REWRITE
                            // elements: call
                            // token labels: 
                            // rule labels: call, retval
                            // token list labels: 
                            // rule list labels: 
                            // wildcard labels: 
                            if ( state.backtracking==0 ) {

                            retval.tree = root_0;
                            RewriteRuleSubtreeStream stream_call=new RewriteRuleSubtreeStream(adaptor,"rule call",call!=null?call.tree:null);
                            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                            root_0 = (EvaluationExpression)adaptor.nil();
                            // 226:8: -> ^( EXPRESSION[\"TernaryExpression\"] $call)
                            {
                                // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:226:11: ^( EXPRESSION[\"TernaryExpression\"] $call)
                                {
                                EvaluationExpression root_1 = (EvaluationExpression)adaptor.nil();
                                root_1 = (EvaluationExpression)adaptor.becomeRoot(
                                (EvaluationExpression)adaptor.create(EXPRESSION, "TernaryExpression")
                                , root_1);

                                adaptor.addChild(root_1, new NotNullOrMissingBooleanExpression().withInputExpression(inExp));

                                adaptor.addChild(root_1, stream_call.nextTree());

                                adaptor.addChild(root_1, inExp);

                                adaptor.addChild(root_0, root_1);
                                }

                            }


                            retval.tree = root_0;
                            }

                            }
                            break;

                    }


                    }
                    break;
                case 2 :
                    // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:228:5: ( '.' ID '(' )=> '.' call= methodCall[inExp] ( ( pathSegment )=>path= pathExpression[$call.tree] -> $path| -> $call)
                    {
                    char_literal50=(Token)match(input,48,FOLLOW_48_in_pathExpression1225); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_48.add(char_literal50);


                    pushFollow(FOLLOW_methodCall_in_pathExpression1229);
                    call=methodCall(inExp);

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_methodCall.add(call.getTree());

                    // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:229:7: ( ( pathSegment )=>path= pathExpression[$call.tree] -> $path| -> $call)
                    int alt29=2;
                    int LA29_0 = input.LA(1);

                    if ( (LA29_0==58) && (synpred12_Meteor())) {
                        alt29=1;
                    }
                    else if ( (LA29_0==48) && (synpred12_Meteor())) {
                        alt29=1;
                    }
                    else if ( (LA29_0==59) && (synpred12_Meteor())) {
                        alt29=1;
                    }
                    else if ( (LA29_0==EOF||LA29_0==AND||(LA29_0 >= ID && LA29_0 <= IN)||LA29_0==NOT||LA29_0==OR||(LA29_0 >= SLASH && LA29_0 <= STAR)||LA29_0==37||LA29_0==39||(LA29_0 >= 42 && LA29_0 <= 43)||(LA29_0 >= 45 && LA29_0 <= 46)||(LA29_0 >= 49 && LA29_0 <= 52)||(LA29_0 >= 54 && LA29_0 <= 57)||LA29_0==60||(LA29_0 >= 68 && LA29_0 <= 69)) ) {
                        alt29=2;
                    }
                    else {
                        if (state.backtracking>0) {state.failed=true; return retval;}
                        NoViableAltException nvae =
                            new NoViableAltException("", 29, 0, input);

                        throw nvae;

                    }
                    switch (alt29) {
                        case 1 :
                            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:229:8: ( pathSegment )=>path= pathExpression[$call.tree]
                            {
                            pushFollow(FOLLOW_pathExpression_in_pathExpression1246);
                            path=pathExpression((call!=null?((EvaluationExpression)call.tree):null));

                            state._fsp--;
                            if (state.failed) return retval;
                            if ( state.backtracking==0 ) stream_pathExpression.add(path.getTree());

                            // AST REWRITE
                            // elements: path
                            // token labels: 
                            // rule labels: retval, path
                            // token list labels: 
                            // rule list labels: 
                            // wildcard labels: 
                            if ( state.backtracking==0 ) {

                            retval.tree = root_0;
                            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);
                            RewriteRuleSubtreeStream stream_path=new RewriteRuleSubtreeStream(adaptor,"rule path",path!=null?path.tree:null);

                            root_0 = (EvaluationExpression)adaptor.nil();
                            // 229:55: -> $path
                            {
                                adaptor.addChild(root_0, stream_path.nextTree());

                            }


                            retval.tree = root_0;
                            }

                            }
                            break;
                        case 2 :
                            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:229:66: 
                            {
                            // AST REWRITE
                            // elements: call
                            // token labels: 
                            // rule labels: call, retval
                            // token list labels: 
                            // rule list labels: 
                            // wildcard labels: 
                            if ( state.backtracking==0 ) {

                            retval.tree = root_0;
                            RewriteRuleSubtreeStream stream_call=new RewriteRuleSubtreeStream(adaptor,"rule call",call!=null?call.tree:null);
                            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                            root_0 = (EvaluationExpression)adaptor.nil();
                            // 229:66: -> $call
                            {
                                adaptor.addChild(root_0, stream_call.nextTree());

                            }


                            retval.tree = root_0;
                            }

                            }
                            break;

                    }


                    }
                    break;
                case 3 :
                    // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:231:5: seg= pathSegment ( ( pathSegment )=>path= pathExpression[$seg.tree] -> $path| -> $seg)
                    {
                    pushFollow(FOLLOW_pathSegment_in_pathExpression1272);
                    seg=pathSegment();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_pathSegment.add(seg.getTree());

                    if ( state.backtracking==0 ) { ((PathSegmentExpression) seg.getTree()).setInputExpression(inExp); }

                    // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:232:5: ( ( pathSegment )=>path= pathExpression[$seg.tree] -> $path| -> $seg)
                    int alt30=2;
                    int LA30_0 = input.LA(1);

                    if ( (LA30_0==58) && (synpred13_Meteor())) {
                        alt30=1;
                    }
                    else if ( (LA30_0==48) && (synpred13_Meteor())) {
                        alt30=1;
                    }
                    else if ( (LA30_0==59) && (synpred13_Meteor())) {
                        alt30=1;
                    }
                    else if ( (LA30_0==EOF||LA30_0==AND||(LA30_0 >= ID && LA30_0 <= IN)||LA30_0==NOT||LA30_0==OR||(LA30_0 >= SLASH && LA30_0 <= STAR)||LA30_0==37||LA30_0==39||(LA30_0 >= 42 && LA30_0 <= 43)||(LA30_0 >= 45 && LA30_0 <= 46)||(LA30_0 >= 49 && LA30_0 <= 52)||(LA30_0 >= 54 && LA30_0 <= 57)||LA30_0==60||(LA30_0 >= 68 && LA30_0 <= 69)) ) {
                        alt30=2;
                    }
                    else {
                        if (state.backtracking>0) {state.failed=true; return retval;}
                        NoViableAltException nvae =
                            new NoViableAltException("", 30, 0, input);

                        throw nvae;

                    }
                    switch (alt30) {
                        case 1 :
                            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:232:6: ( pathSegment )=>path= pathExpression[$seg.tree]
                            {
                            pushFollow(FOLLOW_pathExpression_in_pathExpression1288);
                            path=pathExpression((seg!=null?((EvaluationExpression)seg.tree):null));

                            state._fsp--;
                            if (state.failed) return retval;
                            if ( state.backtracking==0 ) stream_pathExpression.add(path.getTree());

                            // AST REWRITE
                            // elements: path
                            // token labels: 
                            // rule labels: retval, path
                            // token list labels: 
                            // rule list labels: 
                            // wildcard labels: 
                            if ( state.backtracking==0 ) {

                            retval.tree = root_0;
                            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);
                            RewriteRuleSubtreeStream stream_path=new RewriteRuleSubtreeStream(adaptor,"rule path",path!=null?path.tree:null);

                            root_0 = (EvaluationExpression)adaptor.nil();
                            // 232:53: -> $path
                            {
                                adaptor.addChild(root_0, stream_path.nextTree());

                            }


                            retval.tree = root_0;
                            }

                            }
                            break;
                        case 2 :
                            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:232:64: 
                            {
                            // AST REWRITE
                            // elements: seg
                            // token labels: 
                            // rule labels: retval, seg
                            // token list labels: 
                            // rule list labels: 
                            // wildcard labels: 
                            if ( state.backtracking==0 ) {

                            retval.tree = root_0;
                            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);
                            RewriteRuleSubtreeStream stream_seg=new RewriteRuleSubtreeStream(adaptor,"rule seg",seg!=null?seg.tree:null);

                            root_0 = (EvaluationExpression)adaptor.nil();
                            // 232:64: -> $seg
                            {
                                adaptor.addChild(root_0, stream_seg.nextTree());

                            }


                            retval.tree = root_0;
                            }

                            }
                            break;

                    }


                    }
                    break;

            }
            retval.stop = input.LT(-1);


            if ( state.backtracking==0 ) {

            retval.tree = (EvaluationExpression)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
        catch (NoViableAltException re) {
             explainUsage("in a path expression only .field, ?.field, [...], and .method(...) are allowed", re); 
        }

        finally {
        	// do for sure before leaving
        }
        return retval;
    }
    // $ANTLR end "pathExpression"


    public static class pathSegment_return extends ParserRuleReturnScope {
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };


    // $ANTLR start "pathSegment"
    // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:235:1: pathSegment : ( ( '?.' )=> '?.' field= ID -> ^( EXPRESSION[\"TernaryExpression\"] ) | ( '.' )=> '.' field= ID -> ^( EXPRESSION[\"ObjectAccess\"] ) | ( '[' )=> arrayAccess );
    public final MeteorParser.pathSegment_return pathSegment() throws RecognitionException {
        MeteorParser.pathSegment_return retval = new MeteorParser.pathSegment_return();
        retval.start = input.LT(1);


        EvaluationExpression root_0 = null;

        Token field=null;
        Token string_literal51=null;
        Token char_literal52=null;
        MeteorParser.arrayAccess_return arrayAccess53 =null;


        EvaluationExpression field_tree=null;
        EvaluationExpression string_literal51_tree=null;
        EvaluationExpression char_literal52_tree=null;
        RewriteRuleTokenStream stream_48=new RewriteRuleTokenStream(adaptor,"token 48");
        RewriteRuleTokenStream stream_58=new RewriteRuleTokenStream(adaptor,"token 58");
        RewriteRuleTokenStream stream_ID=new RewriteRuleTokenStream(adaptor,"token ID");

          paraphrase.push("a path expression"); 
        try {
            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:238:3: ( ( '?.' )=> '?.' field= ID -> ^( EXPRESSION[\"TernaryExpression\"] ) | ( '.' )=> '.' field= ID -> ^( EXPRESSION[\"ObjectAccess\"] ) | ( '[' )=> arrayAccess )
            int alt32=3;
            int LA32_0 = input.LA(1);

            if ( (LA32_0==58) && (synpred14_Meteor())) {
                alt32=1;
            }
            else if ( (LA32_0==48) && (synpred15_Meteor())) {
                alt32=2;
            }
            else if ( (LA32_0==59) && (synpred16_Meteor())) {
                alt32=3;
            }
            else {
                if (state.backtracking>0) {state.failed=true; return retval;}
                NoViableAltException nvae =
                    new NoViableAltException("", 32, 0, input);

                throw nvae;

            }
            switch (alt32) {
                case 1 :
                    // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:239:5: ( '?.' )=> '?.' field= ID
                    {
                    string_literal51=(Token)match(input,58,FOLLOW_58_in_pathSegment1340); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_58.add(string_literal51);


                    field=(Token)match(input,ID,FOLLOW_ID_in_pathSegment1344); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_ID.add(field);


                    // AST REWRITE
                    // elements: 
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    if ( state.backtracking==0 ) {

                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (EvaluationExpression)adaptor.nil();
                    // 239:28: -> ^( EXPRESSION[\"TernaryExpression\"] )
                    {
                        // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:239:31: ^( EXPRESSION[\"TernaryExpression\"] )
                        {
                        EvaluationExpression root_1 = (EvaluationExpression)adaptor.nil();
                        root_1 = (EvaluationExpression)adaptor.becomeRoot(
                        (EvaluationExpression)adaptor.create(EXPRESSION, "TernaryExpression")
                        , root_1);

                        adaptor.addChild(root_1, new NotNullOrMissingBooleanExpression());

                        adaptor.addChild(root_1, new ObjectAccess((field!=null?field.getText():null)));

                        adaptor.addChild(root_1, EvaluationExpression.VALUE);

                        adaptor.addChild(root_0, root_1);
                        }

                    }


                    retval.tree = root_0;
                    }

                    }
                    break;
                case 2 :
                    // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:240:5: ( '.' )=> '.' field= ID
                    {
                    char_literal52=(Token)match(input,48,FOLLOW_48_in_pathSegment1372); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_48.add(char_literal52);


                    field=(Token)match(input,ID,FOLLOW_ID_in_pathSegment1376); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_ID.add(field);


                    // AST REWRITE
                    // elements: 
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    if ( state.backtracking==0 ) {

                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (EvaluationExpression)adaptor.nil();
                    // 240:27: -> ^( EXPRESSION[\"ObjectAccess\"] )
                    {
                        // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:240:30: ^( EXPRESSION[\"ObjectAccess\"] )
                        {
                        EvaluationExpression root_1 = (EvaluationExpression)adaptor.nil();
                        root_1 = (EvaluationExpression)adaptor.becomeRoot(
                        (EvaluationExpression)adaptor.create(EXPRESSION, "ObjectAccess")
                        , root_1);

                        adaptor.addChild(root_1, (field!=null?field.getText():null));

                        adaptor.addChild(root_0, root_1);
                        }

                    }


                    retval.tree = root_0;
                    }

                    }
                    break;
                case 3 :
                    // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:241:5: ( '[' )=> arrayAccess
                    {
                    root_0 = (EvaluationExpression)adaptor.nil();


                    pushFollow(FOLLOW_arrayAccess_in_pathSegment1401);
                    arrayAccess53=arrayAccess();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, arrayAccess53.getTree());

                    }
                    break;

            }
            retval.stop = input.LT(-1);


            if ( state.backtracking==0 ) {

            retval.tree = (EvaluationExpression)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
            if ( state.backtracking==0 ) { paraphrase.pop(); }
        }

        catch (RecognitionException e) {
          throw e;
        }

        finally {
        	// do for sure before leaving
        }
        return retval;
    }
    // $ANTLR end "pathSegment"


    public static class arrayAccess_return extends ParserRuleReturnScope {
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };


    // $ANTLR start "arrayAccess"
    // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:243:1: arrayAccess : ( '[' STAR ']' ( ( '.' methodCall[null] )=> '.' call= methodCall[EvaluationExpression.VALUE] -> ^( EXPRESSION[\"ArrayProjection\"] $call) |path= pathSegment -> ^( EXPRESSION[\"ArrayProjection\"] $path) ) | '[' (pos= INTEGER |pos= UINT ) ']' -> ^( EXPRESSION[\"ArrayAccess\"] ) | '[' (start= INTEGER |start= UINT ) ':' (end= INTEGER |end= UINT ) ']' -> ^( EXPRESSION[\"ArrayAccess\"] ) );
    public final MeteorParser.arrayAccess_return arrayAccess() throws RecognitionException {
        MeteorParser.arrayAccess_return retval = new MeteorParser.arrayAccess_return();
        retval.start = input.LT(1);


        EvaluationExpression root_0 = null;

        Token pos=null;
        Token start=null;
        Token end=null;
        Token char_literal54=null;
        Token STAR55=null;
        Token char_literal56=null;
        Token char_literal57=null;
        Token char_literal58=null;
        Token char_literal59=null;
        Token char_literal60=null;
        Token char_literal61=null;
        Token char_literal62=null;
        MeteorParser.methodCall_return call =null;

        MeteorParser.pathSegment_return path =null;


        EvaluationExpression pos_tree=null;
        EvaluationExpression start_tree=null;
        EvaluationExpression end_tree=null;
        EvaluationExpression char_literal54_tree=null;
        EvaluationExpression STAR55_tree=null;
        EvaluationExpression char_literal56_tree=null;
        EvaluationExpression char_literal57_tree=null;
        EvaluationExpression char_literal58_tree=null;
        EvaluationExpression char_literal59_tree=null;
        EvaluationExpression char_literal60_tree=null;
        EvaluationExpression char_literal61_tree=null;
        EvaluationExpression char_literal62_tree=null;
        RewriteRuleTokenStream stream_49=new RewriteRuleTokenStream(adaptor,"token 49");
        RewriteRuleTokenStream stream_INTEGER=new RewriteRuleTokenStream(adaptor,"token INTEGER");
        RewriteRuleTokenStream stream_48=new RewriteRuleTokenStream(adaptor,"token 48");
        RewriteRuleTokenStream stream_59=new RewriteRuleTokenStream(adaptor,"token 59");
        RewriteRuleTokenStream stream_STAR=new RewriteRuleTokenStream(adaptor,"token STAR");
        RewriteRuleTokenStream stream_UINT=new RewriteRuleTokenStream(adaptor,"token UINT");
        RewriteRuleTokenStream stream_60=new RewriteRuleTokenStream(adaptor,"token 60");
        RewriteRuleSubtreeStream stream_methodCall=new RewriteRuleSubtreeStream(adaptor,"rule methodCall");
        RewriteRuleSubtreeStream stream_pathSegment=new RewriteRuleSubtreeStream(adaptor,"rule pathSegment");
        try {
            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:244:3: ( '[' STAR ']' ( ( '.' methodCall[null] )=> '.' call= methodCall[EvaluationExpression.VALUE] -> ^( EXPRESSION[\"ArrayProjection\"] $call) |path= pathSegment -> ^( EXPRESSION[\"ArrayProjection\"] $path) ) | '[' (pos= INTEGER |pos= UINT ) ']' -> ^( EXPRESSION[\"ArrayAccess\"] ) | '[' (start= INTEGER |start= UINT ) ':' (end= INTEGER |end= UINT ) ']' -> ^( EXPRESSION[\"ArrayAccess\"] ) )
            int alt37=3;
            int LA37_0 = input.LA(1);

            if ( (LA37_0==59) ) {
                switch ( input.LA(2) ) {
                case STAR:
                    {
                    alt37=1;
                    }
                    break;
                case INTEGER:
                    {
                    int LA37_3 = input.LA(3);

                    if ( (LA37_3==60) ) {
                        alt37=2;
                    }
                    else if ( (LA37_3==49) ) {
                        alt37=3;
                    }
                    else {
                        if (state.backtracking>0) {state.failed=true; return retval;}
                        NoViableAltException nvae =
                            new NoViableAltException("", 37, 3, input);

                        throw nvae;

                    }
                    }
                    break;
                case UINT:
                    {
                    int LA37_4 = input.LA(3);

                    if ( (LA37_4==60) ) {
                        alt37=2;
                    }
                    else if ( (LA37_4==49) ) {
                        alt37=3;
                    }
                    else {
                        if (state.backtracking>0) {state.failed=true; return retval;}
                        NoViableAltException nvae =
                            new NoViableAltException("", 37, 4, input);

                        throw nvae;

                    }
                    }
                    break;
                default:
                    if (state.backtracking>0) {state.failed=true; return retval;}
                    NoViableAltException nvae =
                        new NoViableAltException("", 37, 1, input);

                    throw nvae;

                }

            }
            else {
                if (state.backtracking>0) {state.failed=true; return retval;}
                NoViableAltException nvae =
                    new NoViableAltException("", 37, 0, input);

                throw nvae;

            }
            switch (alt37) {
                case 1 :
                    // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:244:5: '[' STAR ']' ( ( '.' methodCall[null] )=> '.' call= methodCall[EvaluationExpression.VALUE] -> ^( EXPRESSION[\"ArrayProjection\"] $call) |path= pathSegment -> ^( EXPRESSION[\"ArrayProjection\"] $path) )
                    {
                    char_literal54=(Token)match(input,59,FOLLOW_59_in_arrayAccess1411); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_59.add(char_literal54);


                    STAR55=(Token)match(input,STAR,FOLLOW_STAR_in_arrayAccess1413); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_STAR.add(STAR55);


                    char_literal56=(Token)match(input,60,FOLLOW_60_in_arrayAccess1415); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_60.add(char_literal56);


                    // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:244:18: ( ( '.' methodCall[null] )=> '.' call= methodCall[EvaluationExpression.VALUE] -> ^( EXPRESSION[\"ArrayProjection\"] $call) |path= pathSegment -> ^( EXPRESSION[\"ArrayProjection\"] $path) )
                    int alt33=2;
                    int LA33_0 = input.LA(1);

                    if ( (LA33_0==48) ) {
                        int LA33_1 = input.LA(2);

                        if ( (synpred17_Meteor()) ) {
                            alt33=1;
                        }
                        else if ( (true) ) {
                            alt33=2;
                        }
                        else {
                            if (state.backtracking>0) {state.failed=true; return retval;}
                            NoViableAltException nvae =
                                new NoViableAltException("", 33, 1, input);

                            throw nvae;

                        }
                    }
                    else if ( ((LA33_0 >= 58 && LA33_0 <= 59)) ) {
                        alt33=2;
                    }
                    else {
                        if (state.backtracking>0) {state.failed=true; return retval;}
                        NoViableAltException nvae =
                            new NoViableAltException("", 33, 0, input);

                        throw nvae;

                    }
                    switch (alt33) {
                        case 1 :
                            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:244:19: ( '.' methodCall[null] )=> '.' call= methodCall[EvaluationExpression.VALUE]
                            {
                            char_literal57=(Token)match(input,48,FOLLOW_48_in_arrayAccess1426); if (state.failed) return retval; 
                            if ( state.backtracking==0 ) stream_48.add(char_literal57);


                            pushFollow(FOLLOW_methodCall_in_arrayAccess1430);
                            call=methodCall(EvaluationExpression.VALUE);

                            state._fsp--;
                            if (state.failed) return retval;
                            if ( state.backtracking==0 ) stream_methodCall.add(call.getTree());

                            // AST REWRITE
                            // elements: call
                            // token labels: 
                            // rule labels: call, retval
                            // token list labels: 
                            // rule list labels: 
                            // wildcard labels: 
                            if ( state.backtracking==0 ) {

                            retval.tree = root_0;
                            RewriteRuleSubtreeStream stream_call=new RewriteRuleSubtreeStream(adaptor,"rule call",call!=null?call.tree:null);
                            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                            root_0 = (EvaluationExpression)adaptor.nil();
                            // 245:4: -> ^( EXPRESSION[\"ArrayProjection\"] $call)
                            {
                                // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:245:7: ^( EXPRESSION[\"ArrayProjection\"] $call)
                                {
                                EvaluationExpression root_1 = (EvaluationExpression)adaptor.nil();
                                root_1 = (EvaluationExpression)adaptor.becomeRoot(
                                (EvaluationExpression)adaptor.create(EXPRESSION, "ArrayProjection")
                                , root_1);

                                adaptor.addChild(root_1, stream_call.nextTree());

                                adaptor.addChild(root_0, root_1);
                                }

                            }


                            retval.tree = root_0;
                            }

                            }
                            break;
                        case 2 :
                            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:246:6: path= pathSegment
                            {
                            pushFollow(FOLLOW_pathSegment_in_arrayAccess1453);
                            path=pathSegment();

                            state._fsp--;
                            if (state.failed) return retval;
                            if ( state.backtracking==0 ) stream_pathSegment.add(path.getTree());

                            // AST REWRITE
                            // elements: path
                            // token labels: 
                            // rule labels: retval, path
                            // token list labels: 
                            // rule list labels: 
                            // wildcard labels: 
                            if ( state.backtracking==0 ) {

                            retval.tree = root_0;
                            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);
                            RewriteRuleSubtreeStream stream_path=new RewriteRuleSubtreeStream(adaptor,"rule path",path!=null?path.tree:null);

                            root_0 = (EvaluationExpression)adaptor.nil();
                            // 247:4: -> ^( EXPRESSION[\"ArrayProjection\"] $path)
                            {
                                // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:247:7: ^( EXPRESSION[\"ArrayProjection\"] $path)
                                {
                                EvaluationExpression root_1 = (EvaluationExpression)adaptor.nil();
                                root_1 = (EvaluationExpression)adaptor.becomeRoot(
                                (EvaluationExpression)adaptor.create(EXPRESSION, "ArrayProjection")
                                , root_1);

                                adaptor.addChild(root_1, stream_path.nextTree());

                                adaptor.addChild(root_0, root_1);
                                }

                            }


                            retval.tree = root_0;
                            }

                            }
                            break;

                    }


                    }
                    break;
                case 2 :
                    // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:248:5: '[' (pos= INTEGER |pos= UINT ) ']'
                    {
                    char_literal58=(Token)match(input,59,FOLLOW_59_in_arrayAccess1474); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_59.add(char_literal58);


                    // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:248:9: (pos= INTEGER |pos= UINT )
                    int alt34=2;
                    int LA34_0 = input.LA(1);

                    if ( (LA34_0==INTEGER) ) {
                        alt34=1;
                    }
                    else if ( (LA34_0==UINT) ) {
                        alt34=2;
                    }
                    else {
                        if (state.backtracking>0) {state.failed=true; return retval;}
                        NoViableAltException nvae =
                            new NoViableAltException("", 34, 0, input);

                        throw nvae;

                    }
                    switch (alt34) {
                        case 1 :
                            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:248:10: pos= INTEGER
                            {
                            pos=(Token)match(input,INTEGER,FOLLOW_INTEGER_in_arrayAccess1479); if (state.failed) return retval; 
                            if ( state.backtracking==0 ) stream_INTEGER.add(pos);


                            }
                            break;
                        case 2 :
                            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:248:24: pos= UINT
                            {
                            pos=(Token)match(input,UINT,FOLLOW_UINT_in_arrayAccess1485); if (state.failed) return retval; 
                            if ( state.backtracking==0 ) stream_UINT.add(pos);


                            }
                            break;

                    }


                    char_literal59=(Token)match(input,60,FOLLOW_60_in_arrayAccess1488); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_60.add(char_literal59);


                    // AST REWRITE
                    // elements: 
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    if ( state.backtracking==0 ) {

                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (EvaluationExpression)adaptor.nil();
                    // 249:3: -> ^( EXPRESSION[\"ArrayAccess\"] )
                    {
                        // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:249:6: ^( EXPRESSION[\"ArrayAccess\"] )
                        {
                        EvaluationExpression root_1 = (EvaluationExpression)adaptor.nil();
                        root_1 = (EvaluationExpression)adaptor.becomeRoot(
                        (EvaluationExpression)adaptor.create(EXPRESSION, "ArrayAccess")
                        , root_1);

                        adaptor.addChild(root_1,  Integer.valueOf((pos!=null?pos.getText():null)) );

                        adaptor.addChild(root_0, root_1);
                        }

                    }


                    retval.tree = root_0;
                    }

                    }
                    break;
                case 3 :
                    // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:250:5: '[' (start= INTEGER |start= UINT ) ':' (end= INTEGER |end= UINT ) ']'
                    {
                    char_literal60=(Token)match(input,59,FOLLOW_59_in_arrayAccess1506); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_59.add(char_literal60);


                    // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:250:9: (start= INTEGER |start= UINT )
                    int alt35=2;
                    int LA35_0 = input.LA(1);

                    if ( (LA35_0==INTEGER) ) {
                        alt35=1;
                    }
                    else if ( (LA35_0==UINT) ) {
                        alt35=2;
                    }
                    else {
                        if (state.backtracking>0) {state.failed=true; return retval;}
                        NoViableAltException nvae =
                            new NoViableAltException("", 35, 0, input);

                        throw nvae;

                    }
                    switch (alt35) {
                        case 1 :
                            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:250:10: start= INTEGER
                            {
                            start=(Token)match(input,INTEGER,FOLLOW_INTEGER_in_arrayAccess1511); if (state.failed) return retval; 
                            if ( state.backtracking==0 ) stream_INTEGER.add(start);


                            }
                            break;
                        case 2 :
                            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:250:26: start= UINT
                            {
                            start=(Token)match(input,UINT,FOLLOW_UINT_in_arrayAccess1517); if (state.failed) return retval; 
                            if ( state.backtracking==0 ) stream_UINT.add(start);


                            }
                            break;

                    }


                    char_literal61=(Token)match(input,49,FOLLOW_49_in_arrayAccess1520); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_49.add(char_literal61);


                    // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:250:42: (end= INTEGER |end= UINT )
                    int alt36=2;
                    int LA36_0 = input.LA(1);

                    if ( (LA36_0==INTEGER) ) {
                        alt36=1;
                    }
                    else if ( (LA36_0==UINT) ) {
                        alt36=2;
                    }
                    else {
                        if (state.backtracking>0) {state.failed=true; return retval;}
                        NoViableAltException nvae =
                            new NoViableAltException("", 36, 0, input);

                        throw nvae;

                    }
                    switch (alt36) {
                        case 1 :
                            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:250:43: end= INTEGER
                            {
                            end=(Token)match(input,INTEGER,FOLLOW_INTEGER_in_arrayAccess1525); if (state.failed) return retval; 
                            if ( state.backtracking==0 ) stream_INTEGER.add(end);


                            }
                            break;
                        case 2 :
                            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:250:57: end= UINT
                            {
                            end=(Token)match(input,UINT,FOLLOW_UINT_in_arrayAccess1531); if (state.failed) return retval; 
                            if ( state.backtracking==0 ) stream_UINT.add(end);


                            }
                            break;

                    }


                    char_literal62=(Token)match(input,60,FOLLOW_60_in_arrayAccess1534); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_60.add(char_literal62);


                    // AST REWRITE
                    // elements: 
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    if ( state.backtracking==0 ) {

                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (EvaluationExpression)adaptor.nil();
                    // 251:3: -> ^( EXPRESSION[\"ArrayAccess\"] )
                    {
                        // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:251:6: ^( EXPRESSION[\"ArrayAccess\"] )
                        {
                        EvaluationExpression root_1 = (EvaluationExpression)adaptor.nil();
                        root_1 = (EvaluationExpression)adaptor.becomeRoot(
                        (EvaluationExpression)adaptor.create(EXPRESSION, "ArrayAccess")
                        , root_1);

                        adaptor.addChild(root_1,  Integer.valueOf((start!=null?start.getText():null)) );

                        adaptor.addChild(root_1,  Integer.valueOf((end!=null?end.getText():null)) );

                        adaptor.addChild(root_0, root_1);
                        }

                    }


                    retval.tree = root_0;
                    }

                    }
                    break;

            }
            retval.stop = input.LT(-1);


            if ( state.backtracking==0 ) {

            retval.tree = (EvaluationExpression)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }

        catch (RecognitionException e) {
          throw e;
        }

        finally {
        	// do for sure before leaving
        }
        return retval;
    }
    // $ANTLR end "arrayAccess"


    public static class valueExpression_return extends ParserRuleReturnScope {
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };


    // $ANTLR start "valueExpression"
    // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:253:1: valueExpression : ( ( ID '(' )=> functionCall | functionReference | ( FN )=>func= inlineFunction -> ^( EXPRESSION[\"ConstantExpression\"] ) | parenthesesExpression | literal | VAR ->| constantExpression | arrayCreation | objectCreation );
    public final MeteorParser.valueExpression_return valueExpression() throws RecognitionException {
        MeteorParser.valueExpression_return retval = new MeteorParser.valueExpression_return();
        retval.start = input.LT(1);


        EvaluationExpression root_0 = null;

        Token VAR67=null;
        MeteorParser.inlineFunction_return func =null;

        MeteorParser.functionCall_return functionCall63 =null;

        MeteorParser.functionReference_return functionReference64 =null;

        MeteorParser.parenthesesExpression_return parenthesesExpression65 =null;

        MeteorParser.literal_return literal66 =null;

        MeteorParser.constantExpression_return constantExpression68 =null;

        MeteorParser.arrayCreation_return arrayCreation69 =null;

        MeteorParser.objectCreation_return objectCreation70 =null;


        EvaluationExpression VAR67_tree=null;
        RewriteRuleTokenStream stream_VAR=new RewriteRuleTokenStream(adaptor,"token VAR");
        RewriteRuleSubtreeStream stream_inlineFunction=new RewriteRuleSubtreeStream(adaptor,"rule inlineFunction");
        try {
            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:254:2: ( ( ID '(' )=> functionCall | functionReference | ( FN )=>func= inlineFunction -> ^( EXPRESSION[\"ConstantExpression\"] ) | parenthesesExpression | literal | VAR ->| constantExpression | arrayCreation | objectCreation )
            int alt38=9;
            int LA38_0 = input.LA(1);

            if ( (LA38_0==ID) ) {
                int LA38_1 = input.LA(2);

                if ( (LA38_1==49) ) {
                    int LA38_9 = input.LA(3);

                    if ( (synpred18_Meteor()) ) {
                        alt38=1;
                    }
                    else if ( (true) ) {
                        alt38=7;
                    }
                    else {
                        if (state.backtracking>0) {state.failed=true; return retval;}
                        NoViableAltException nvae =
                            new NoViableAltException("", 38, 9, input);

                        throw nvae;

                    }
                }
                else if ( (LA38_1==41) && (synpred18_Meteor())) {
                    alt38=1;
                }
                else if ( (LA38_1==EOF||LA38_1==AND||(LA38_1 >= ID && LA38_1 <= IN)||LA38_1==NOT||LA38_1==OR||(LA38_1 >= SLASH && LA38_1 <= STAR)||LA38_1==37||LA38_1==39||(LA38_1 >= 42 && LA38_1 <= 43)||(LA38_1 >= 45 && LA38_1 <= 46)||LA38_1==48||(LA38_1 >= 50 && LA38_1 <= 52)||(LA38_1 >= 54 && LA38_1 <= 60)||(LA38_1 >= 68 && LA38_1 <= 69)) ) {
                    alt38=7;
                }
                else {
                    if (state.backtracking>0) {state.failed=true; return retval;}
                    NoViableAltException nvae =
                        new NoViableAltException("", 38, 1, input);

                    throw nvae;

                }
            }
            else if ( (LA38_0==40) ) {
                alt38=2;
            }
            else if ( (LA38_0==FN) && (synpred19_Meteor())) {
                alt38=3;
            }
            else if ( (LA38_0==41) ) {
                alt38=4;
            }
            else if ( (LA38_0==DECIMAL||LA38_0==INTEGER||(LA38_0 >= STRING && LA38_0 <= UINT)||(LA38_0 >= 61 && LA38_0 <= 62)||LA38_0==64) ) {
                alt38=5;
            }
            else if ( (LA38_0==VAR) ) {
                alt38=6;
            }
            else if ( (LA38_0==59) ) {
                alt38=8;
            }
            else if ( (LA38_0==67) ) {
                alt38=9;
            }
            else {
                if (state.backtracking>0) {state.failed=true; return retval;}
                NoViableAltException nvae =
                    new NoViableAltException("", 38, 0, input);

                throw nvae;

            }
            switch (alt38) {
                case 1 :
                    // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:254:4: ( ID '(' )=> functionCall
                    {
                    root_0 = (EvaluationExpression)adaptor.nil();


                    pushFollow(FOLLOW_functionCall_in_valueExpression1566);
                    functionCall63=functionCall();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, functionCall63.getTree());

                    }
                    break;
                case 2 :
                    // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:255:4: functionReference
                    {
                    root_0 = (EvaluationExpression)adaptor.nil();


                    pushFollow(FOLLOW_functionReference_in_valueExpression1571);
                    functionReference64=functionReference();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, functionReference64.getTree());

                    }
                    break;
                case 3 :
                    // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:256:5: ( FN )=>func= inlineFunction
                    {
                    pushFollow(FOLLOW_inlineFunction_in_valueExpression1584);
                    func=inlineFunction();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_inlineFunction.add(func.getTree());

                    // AST REWRITE
                    // elements: 
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    if ( state.backtracking==0 ) {

                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (EvaluationExpression)adaptor.nil();
                    // 256:32: -> ^( EXPRESSION[\"ConstantExpression\"] )
                    {
                        // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:256:35: ^( EXPRESSION[\"ConstantExpression\"] )
                        {
                        EvaluationExpression root_1 = (EvaluationExpression)adaptor.nil();
                        root_1 = (EvaluationExpression)adaptor.becomeRoot(
                        (EvaluationExpression)adaptor.create(EXPRESSION, "ConstantExpression")
                        , root_1);

                        adaptor.addChild(root_1,  new FunctionNode((func!=null?func.func:null)) );

                        adaptor.addChild(root_0, root_1);
                        }

                    }


                    retval.tree = root_0;
                    }

                    }
                    break;
                case 4 :
                    // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:257:4: parenthesesExpression
                    {
                    root_0 = (EvaluationExpression)adaptor.nil();


                    pushFollow(FOLLOW_parenthesesExpression_in_valueExpression1598);
                    parenthesesExpression65=parenthesesExpression();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, parenthesesExpression65.getTree());

                    }
                    break;
                case 5 :
                    // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:258:4: literal
                    {
                    root_0 = (EvaluationExpression)adaptor.nil();


                    pushFollow(FOLLOW_literal_in_valueExpression1604);
                    literal66=literal();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, literal66.getTree());

                    }
                    break;
                case 6 :
                    // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:259:4: VAR
                    {
                    VAR67=(Token)match(input,VAR,FOLLOW_VAR_in_valueExpression1610); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_VAR.add(VAR67);


                    // AST REWRITE
                    // elements: 
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    if ( state.backtracking==0 ) {

                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (EvaluationExpression)adaptor.nil();
                    // 259:8: ->
                    {
                        adaptor.addChild(root_0,  getInputSelection(VAR67) );

                    }


                    retval.tree = root_0;
                    }

                    }
                    break;
                case 7 :
                    // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:260:5: constantExpression
                    {
                    root_0 = (EvaluationExpression)adaptor.nil();


                    pushFollow(FOLLOW_constantExpression_in_valueExpression1620);
                    constantExpression68=constantExpression();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, constantExpression68.getTree());

                    }
                    break;
                case 8 :
                    // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:261:4: arrayCreation
                    {
                    root_0 = (EvaluationExpression)adaptor.nil();


                    pushFollow(FOLLOW_arrayCreation_in_valueExpression1627);
                    arrayCreation69=arrayCreation();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, arrayCreation69.getTree());

                    }
                    break;
                case 9 :
                    // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:262:4: objectCreation
                    {
                    root_0 = (EvaluationExpression)adaptor.nil();


                    pushFollow(FOLLOW_objectCreation_in_valueExpression1633);
                    objectCreation70=objectCreation();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, objectCreation70.getTree());

                    }
                    break;

            }
            retval.stop = input.LT(-1);


            if ( state.backtracking==0 ) {

            retval.tree = (EvaluationExpression)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }

        catch (RecognitionException e) {
          throw e;
        }

        finally {
        	// do for sure before leaving
        }
        return retval;
    }
    // $ANTLR end "valueExpression"


    public static class constantExpression_return extends ParserRuleReturnScope {
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };


    // $ANTLR start "constantExpression"
    // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:264:1: constantExpression : ({...}? => ( ID ':' )=>packageName= ID ':' )? constant= ID {...}? => ->;
    public final MeteorParser.constantExpression_return constantExpression() throws RecognitionException {
        MeteorParser.constantExpression_return retval = new MeteorParser.constantExpression_return();
        retval.start = input.LT(1);


        EvaluationExpression root_0 = null;

        Token packageName=null;
        Token constant=null;
        Token char_literal71=null;

        EvaluationExpression packageName_tree=null;
        EvaluationExpression constant_tree=null;
        EvaluationExpression char_literal71_tree=null;
        RewriteRuleTokenStream stream_49=new RewriteRuleTokenStream(adaptor,"token 49");
        RewriteRuleTokenStream stream_ID=new RewriteRuleTokenStream(adaptor,"token ID");

        try {
            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:265:2: ( ({...}? => ( ID ':' )=>packageName= ID ':' )? constant= ID {...}? => ->)
            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:265:4: ({...}? => ( ID ':' )=>packageName= ID ':' )? constant= ID {...}? =>
            {
            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:265:4: ({...}? => ( ID ':' )=>packageName= ID ':' )?
            int alt39=2;
            int LA39_0 = input.LA(1);

            if ( (LA39_0==ID) ) {
                int LA39_1 = input.LA(2);

                if ( (LA39_1==49) ) {
                    int LA39_2 = input.LA(3);

                    if ( ((((ternaryExpression_scope)ternaryExpression_stack.peek()).explicitPackageReferencePossible)) ) {
                        alt39=1;
                    }
                }
            }
            switch (alt39) {
                case 1 :
                    // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:265:5: {...}? => ( ID ':' )=>packageName= ID ':'
                    {
                    if ( !((((ternaryExpression_scope)ternaryExpression_stack.peek()).explicitPackageReferencePossible)) ) {
                        if (state.backtracking>0) {state.failed=true; return retval;}
                        throw new FailedPredicateException(input, "constantExpression", "$ternaryExpression::explicitPackageReferencePossible");
                    }

                    packageName=(Token)match(input,ID,FOLLOW_ID_in_constantExpression1655); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_ID.add(packageName);


                    char_literal71=(Token)match(input,49,FOLLOW_49_in_constantExpression1657); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_49.add(char_literal71);


                    }
                    break;

            }


            constant=(Token)match(input,ID,FOLLOW_ID_in_constantExpression1663); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_ID.add(constant);


            if ( !(( getScope((packageName!=null?packageName.getText():null)).getConstantRegistry().get((constant!=null?constant.getText():null)) != null )) ) {
                if (state.backtracking>0) {state.failed=true; return retval;}
                throw new FailedPredicateException(input, "constantExpression", " getScope($packageName.text).getConstantRegistry().get($constant.text) != null ");
            }

            // AST REWRITE
            // elements: 
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            if ( state.backtracking==0 ) {

            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (EvaluationExpression)adaptor.nil();
            // 266:5: ->
            {
                adaptor.addChild(root_0,  getScope((packageName!=null?packageName.getText():null)).getConstantRegistry().get((constant!=null?constant.getText():null)) );

            }


            retval.tree = root_0;
            }

            }

            retval.stop = input.LT(-1);


            if ( state.backtracking==0 ) {

            retval.tree = (EvaluationExpression)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
        catch (FailedPredicateException fe) {
             explainUsage("Unknown value " + (constant!=null?constant.getText():null), fe); 
        }

        finally {
        	// do for sure before leaving
        }
        return retval;
    }
    // $ANTLR end "constantExpression"


    public static class operatorExpression_return extends ParserRuleReturnScope {
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };


    // $ANTLR start "operatorExpression"
    // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:269:1: operatorExpression : op= operator -> ^( EXPRESSION[\"NestedOperatorExpression\"] ) ;
    public final MeteorParser.operatorExpression_return operatorExpression() throws RecognitionException {
        MeteorParser.operatorExpression_return retval = new MeteorParser.operatorExpression_return();
        retval.start = input.LT(1);


        EvaluationExpression root_0 = null;

        MeteorParser.operator_return op =null;


        RewriteRuleSubtreeStream stream_operator=new RewriteRuleSubtreeStream(adaptor,"rule operator");
        try {
            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:270:2: (op= operator -> ^( EXPRESSION[\"NestedOperatorExpression\"] ) )
            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:270:4: op= operator
            {
            pushFollow(FOLLOW_operator_in_operatorExpression1696);
            op=operator();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_operator.add(op.getTree());

            // AST REWRITE
            // elements: 
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            if ( state.backtracking==0 ) {

            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (EvaluationExpression)adaptor.nil();
            // 270:16: -> ^( EXPRESSION[\"NestedOperatorExpression\"] )
            {
                // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:270:19: ^( EXPRESSION[\"NestedOperatorExpression\"] )
                {
                EvaluationExpression root_1 = (EvaluationExpression)adaptor.nil();
                root_1 = (EvaluationExpression)adaptor.becomeRoot(
                (EvaluationExpression)adaptor.create(EXPRESSION, "NestedOperatorExpression")
                , root_1);

                adaptor.addChild(root_1,  (op!=null?op.op:null) );

                adaptor.addChild(root_0, root_1);
                }

            }


            retval.tree = root_0;
            }

            }

            retval.stop = input.LT(-1);


            if ( state.backtracking==0 ) {

            retval.tree = (EvaluationExpression)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }

        catch (RecognitionException e) {
          throw e;
        }

        finally {
        	// do for sure before leaving
        }
        return retval;
    }
    // $ANTLR end "operatorExpression"


    public static class parenthesesExpression_return extends ParserRuleReturnScope {
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };


    // $ANTLR start "parenthesesExpression"
    // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:272:1: parenthesesExpression : ( '(' expression ')' ) -> expression ;
    public final MeteorParser.parenthesesExpression_return parenthesesExpression() throws RecognitionException {
        MeteorParser.parenthesesExpression_return retval = new MeteorParser.parenthesesExpression_return();
        retval.start = input.LT(1);


        EvaluationExpression root_0 = null;

        Token char_literal72=null;
        Token char_literal74=null;
        MeteorParser.expression_return expression73 =null;


        EvaluationExpression char_literal72_tree=null;
        EvaluationExpression char_literal74_tree=null;
        RewriteRuleTokenStream stream_42=new RewriteRuleTokenStream(adaptor,"token 42");
        RewriteRuleTokenStream stream_41=new RewriteRuleTokenStream(adaptor,"token 41");
        RewriteRuleSubtreeStream stream_expression=new RewriteRuleSubtreeStream(adaptor,"rule expression");
        try {
            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:273:2: ( ( '(' expression ')' ) -> expression )
            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:273:4: ( '(' expression ')' )
            {
            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:273:4: ( '(' expression ')' )
            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:273:5: '(' expression ')'
            {
            char_literal72=(Token)match(input,41,FOLLOW_41_in_parenthesesExpression1715); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_41.add(char_literal72);


            pushFollow(FOLLOW_expression_in_parenthesesExpression1717);
            expression73=expression();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_expression.add(expression73.getTree());

            char_literal74=(Token)match(input,42,FOLLOW_42_in_parenthesesExpression1719); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_42.add(char_literal74);


            }


            // AST REWRITE
            // elements: expression
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            if ( state.backtracking==0 ) {

            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (EvaluationExpression)adaptor.nil();
            // 273:25: -> expression
            {
                adaptor.addChild(root_0, stream_expression.nextTree());

            }


            retval.tree = root_0;
            }

            }

            retval.stop = input.LT(-1);


            if ( state.backtracking==0 ) {

            retval.tree = (EvaluationExpression)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }

        catch (RecognitionException e) {
          throw e;
        }

        finally {
        	// do for sure before leaving
        }
        return retval;
    }
    // $ANTLR end "parenthesesExpression"


    public static class methodCall_return extends ParserRuleReturnScope {
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };


    // $ANTLR start "methodCall"
    // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:275:1: methodCall[EvaluationExpression targetExpr] : (packageName= ID ':' )? name= ID '(' ( (param= expression ) ( ',' (param= expression ) )* )? ')' ->;
    public final MeteorParser.methodCall_return methodCall(EvaluationExpression targetExpr) throws RecognitionException {
        MeteorParser.methodCall_return retval = new MeteorParser.methodCall_return();
        retval.start = input.LT(1);


        EvaluationExpression root_0 = null;

        Token packageName=null;
        Token name=null;
        Token char_literal75=null;
        Token char_literal76=null;
        Token char_literal77=null;
        Token char_literal78=null;
        MeteorParser.expression_return param =null;


        EvaluationExpression packageName_tree=null;
        EvaluationExpression name_tree=null;
        EvaluationExpression char_literal75_tree=null;
        EvaluationExpression char_literal76_tree=null;
        EvaluationExpression char_literal77_tree=null;
        EvaluationExpression char_literal78_tree=null;
        RewriteRuleTokenStream stream_49=new RewriteRuleTokenStream(adaptor,"token 49");
        RewriteRuleTokenStream stream_45=new RewriteRuleTokenStream(adaptor,"token 45");
        RewriteRuleTokenStream stream_42=new RewriteRuleTokenStream(adaptor,"token 42");
        RewriteRuleTokenStream stream_41=new RewriteRuleTokenStream(adaptor,"token 41");
        RewriteRuleTokenStream stream_ID=new RewriteRuleTokenStream(adaptor,"token ID");
        RewriteRuleSubtreeStream stream_expression=new RewriteRuleSubtreeStream(adaptor,"rule expression");
         List<EvaluationExpression> params = new ArrayList();
                paraphrase.push("a method call"); 
        try {
            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:279:3: ( (packageName= ID ':' )? name= ID '(' ( (param= expression ) ( ',' (param= expression ) )* )? ')' ->)
            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:279:5: (packageName= ID ':' )? name= ID '(' ( (param= expression ) ( ',' (param= expression ) )* )? ')'
            {
            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:279:5: (packageName= ID ':' )?
            int alt40=2;
            int LA40_0 = input.LA(1);

            if ( (LA40_0==ID) ) {
                int LA40_1 = input.LA(2);

                if ( (LA40_1==49) ) {
                    alt40=1;
                }
            }
            switch (alt40) {
                case 1 :
                    // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:279:6: packageName= ID ':'
                    {
                    packageName=(Token)match(input,ID,FOLLOW_ID_in_methodCall1749); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_ID.add(packageName);


                    char_literal75=(Token)match(input,49,FOLLOW_49_in_methodCall1751); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_49.add(char_literal75);


                    }
                    break;

            }


            name=(Token)match(input,ID,FOLLOW_ID_in_methodCall1757); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_ID.add(name);


            char_literal76=(Token)match(input,41,FOLLOW_41_in_methodCall1759); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_41.add(char_literal76);


            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:280:3: ( (param= expression ) ( ',' (param= expression ) )* )?
            int alt42=2;
            int LA42_0 = input.LA(1);

            if ( (LA42_0==DECIMAL||LA42_0==FN||LA42_0==ID||LA42_0==INTEGER||(LA42_0 >= STRING && LA42_0 <= UINT)||LA42_0==VAR||LA42_0==36||(LA42_0 >= 40 && LA42_0 <= 41)||LA42_0==44||LA42_0==47||LA42_0==59||(LA42_0 >= 61 && LA42_0 <= 64)||(LA42_0 >= 66 && LA42_0 <= 67)||LA42_0==70) ) {
                alt42=1;
            }
            switch (alt42) {
                case 1 :
                    // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:280:4: (param= expression ) ( ',' (param= expression ) )*
                    {
                    // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:280:4: (param= expression )
                    // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:280:5: param= expression
                    {
                    pushFollow(FOLLOW_expression_in_methodCall1768);
                    param=expression();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_expression.add(param.getTree());

                    if ( state.backtracking==0 ) { params.add((param!=null?((EvaluationExpression)param.tree):null)); }

                    }


                    // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:281:3: ( ',' (param= expression ) )*
                    loop41:
                    do {
                        int alt41=2;
                        int LA41_0 = input.LA(1);

                        if ( (LA41_0==45) ) {
                            alt41=1;
                        }


                        switch (alt41) {
                    	case 1 :
                    	    // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:281:4: ',' (param= expression )
                    	    {
                    	    char_literal77=(Token)match(input,45,FOLLOW_45_in_methodCall1777); if (state.failed) return retval; 
                    	    if ( state.backtracking==0 ) stream_45.add(char_literal77);


                    	    // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:281:8: (param= expression )
                    	    // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:281:9: param= expression
                    	    {
                    	    pushFollow(FOLLOW_expression_in_methodCall1782);
                    	    param=expression();

                    	    state._fsp--;
                    	    if (state.failed) return retval;
                    	    if ( state.backtracking==0 ) stream_expression.add(param.getTree());

                    	    if ( state.backtracking==0 ) { params.add((param!=null?((EvaluationExpression)param.tree):null)); }

                    	    }


                    	    }
                    	    break;

                    	default :
                    	    break loop41;
                        }
                    } while (true);


                    }
                    break;

            }


            char_literal78=(Token)match(input,42,FOLLOW_42_in_methodCall1794); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_42.add(char_literal78);


            // AST REWRITE
            // elements: 
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            if ( state.backtracking==0 ) {

            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (EvaluationExpression)adaptor.nil();
            // 282:7: ->
            {
                adaptor.addChild(root_0,  createCheckedMethodCall((packageName!=null?packageName.getText():null), name, targetExpr, params.toArray(new EvaluationExpression[params.size()])) );

            }


            retval.tree = root_0;
            }

            }

            retval.stop = input.LT(-1);


            if ( state.backtracking==0 ) {

            retval.tree = (EvaluationExpression)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
            if ( state.backtracking==0 ) { paraphrase.pop(); }
        }

        catch (RecognitionException e) {
          throw e;
        }

        finally {
        	// do for sure before leaving
        }
        return retval;
    }
    // $ANTLR end "methodCall"


    public static class functionCall_return extends ParserRuleReturnScope {
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };


    // $ANTLR start "functionCall"
    // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:284:1: functionCall : methodCall[null] ;
    public final MeteorParser.functionCall_return functionCall() throws RecognitionException {
        MeteorParser.functionCall_return retval = new MeteorParser.functionCall_return();
        retval.start = input.LT(1);


        EvaluationExpression root_0 = null;

        MeteorParser.methodCall_return methodCall79 =null;



        try {
            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:285:2: ( methodCall[null] )
            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:285:4: methodCall[null]
            {
            root_0 = (EvaluationExpression)adaptor.nil();


            pushFollow(FOLLOW_methodCall_in_functionCall1809);
            methodCall79=methodCall(null);

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) adaptor.addChild(root_0, methodCall79.getTree());

            }

            retval.stop = input.LT(-1);


            if ( state.backtracking==0 ) {

            retval.tree = (EvaluationExpression)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }

        catch (RecognitionException e) {
          throw e;
        }

        finally {
        	// do for sure before leaving
        }
        return retval;
    }
    // $ANTLR end "functionCall"


    public static class functionReference_return extends ParserRuleReturnScope {
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };


    // $ANTLR start "functionReference"
    // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:287:1: functionReference : '&' ( ( ID ':' )=>packageName= ID ':' )? name= ID -> ^( EXPRESSION[\"ConstantExpression\"] ) ;
    public final MeteorParser.functionReference_return functionReference() throws RecognitionException {
        MeteorParser.functionReference_return retval = new MeteorParser.functionReference_return();
        retval.start = input.LT(1);


        EvaluationExpression root_0 = null;

        Token packageName=null;
        Token name=null;
        Token char_literal80=null;
        Token char_literal81=null;

        EvaluationExpression packageName_tree=null;
        EvaluationExpression name_tree=null;
        EvaluationExpression char_literal80_tree=null;
        EvaluationExpression char_literal81_tree=null;
        RewriteRuleTokenStream stream_49=new RewriteRuleTokenStream(adaptor,"token 49");
        RewriteRuleTokenStream stream_40=new RewriteRuleTokenStream(adaptor,"token 40");
        RewriteRuleTokenStream stream_ID=new RewriteRuleTokenStream(adaptor,"token ID");

        try {
            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:288:3: ( '&' ( ( ID ':' )=>packageName= ID ':' )? name= ID -> ^( EXPRESSION[\"ConstantExpression\"] ) )
            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:288:5: '&' ( ( ID ':' )=>packageName= ID ':' )? name= ID
            {
            char_literal80=(Token)match(input,40,FOLLOW_40_in_functionReference1820); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_40.add(char_literal80);


            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:288:9: ( ( ID ':' )=>packageName= ID ':' )?
            int alt43=2;
            int LA43_0 = input.LA(1);

            if ( (LA43_0==ID) ) {
                int LA43_1 = input.LA(2);

                if ( (LA43_1==49) ) {
                    int LA43_2 = input.LA(3);

                    if ( (LA43_2==ID) ) {
                        int LA43_4 = input.LA(4);

                        if ( (synpred21_Meteor()) ) {
                            alt43=1;
                        }
                    }
                }
            }
            switch (alt43) {
                case 1 :
                    // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:288:10: ( ID ':' )=>packageName= ID ':'
                    {
                    packageName=(Token)match(input,ID,FOLLOW_ID_in_functionReference1832); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_ID.add(packageName);


                    char_literal81=(Token)match(input,49,FOLLOW_49_in_functionReference1834); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_49.add(char_literal81);


                    }
                    break;

            }


            name=(Token)match(input,ID,FOLLOW_ID_in_functionReference1840); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_ID.add(name);


            // AST REWRITE
            // elements: 
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            if ( state.backtracking==0 ) {

            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (EvaluationExpression)adaptor.nil();
            // 289:9: -> ^( EXPRESSION[\"ConstantExpression\"] )
            {
                // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:289:12: ^( EXPRESSION[\"ConstantExpression\"] )
                {
                EvaluationExpression root_1 = (EvaluationExpression)adaptor.nil();
                root_1 = (EvaluationExpression)adaptor.becomeRoot(
                (EvaluationExpression)adaptor.create(EXPRESSION, "ConstantExpression")
                , root_1);

                adaptor.addChild(root_1,  new FunctionNode(getSopremoFunction((packageName!=null?packageName.getText():null), name)) );

                adaptor.addChild(root_0, root_1);
                }

            }


            retval.tree = root_0;
            }

            }

            retval.stop = input.LT(-1);


            if ( state.backtracking==0 ) {

            retval.tree = (EvaluationExpression)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }

        catch (RecognitionException e) {
          throw e;
        }

        finally {
        	// do for sure before leaving
        }
        return retval;
    }
    // $ANTLR end "functionReference"


    public static class fieldAssignment_return extends ParserRuleReturnScope {
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };


    // $ANTLR start "fieldAssignment"
    // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:291:1: fieldAssignment : ( ( ( ID ':' )=> ID ':' expression ->) | ( ( '\"' ID '\"' ':' )=> '\"' ID '\"' ':' expression ->) | ( VAR '.' STAR )=> VAR '.' STAR ->| ( VAR )=>p= generalPathExpression ( ( ':' )=> ':' e2= expression ->| ->) |v= valueExpression ':' e2= expression ->);
    public final MeteorParser.fieldAssignment_return fieldAssignment() throws RecognitionException {
        MeteorParser.fieldAssignment_return retval = new MeteorParser.fieldAssignment_return();
        retval.start = input.LT(1);


        EvaluationExpression root_0 = null;

        Token ID82=null;
        Token char_literal83=null;
        Token char_literal85=null;
        Token ID86=null;
        Token char_literal87=null;
        Token char_literal88=null;
        Token VAR90=null;
        Token char_literal91=null;
        Token STAR92=null;
        Token char_literal93=null;
        Token char_literal94=null;
        MeteorParser.generalPathExpression_return p =null;

        MeteorParser.expression_return e2 =null;

        MeteorParser.valueExpression_return v =null;

        MeteorParser.expression_return expression84 =null;

        MeteorParser.expression_return expression89 =null;


        EvaluationExpression ID82_tree=null;
        EvaluationExpression char_literal83_tree=null;
        EvaluationExpression char_literal85_tree=null;
        EvaluationExpression ID86_tree=null;
        EvaluationExpression char_literal87_tree=null;
        EvaluationExpression char_literal88_tree=null;
        EvaluationExpression VAR90_tree=null;
        EvaluationExpression char_literal91_tree=null;
        EvaluationExpression STAR92_tree=null;
        EvaluationExpression char_literal93_tree=null;
        EvaluationExpression char_literal94_tree=null;
        RewriteRuleTokenStream stream_49=new RewriteRuleTokenStream(adaptor,"token 49");
        RewriteRuleTokenStream stream_48=new RewriteRuleTokenStream(adaptor,"token 48");
        RewriteRuleTokenStream stream_VAR=new RewriteRuleTokenStream(adaptor,"token VAR");
        RewriteRuleTokenStream stream_STAR=new RewriteRuleTokenStream(adaptor,"token STAR");
        RewriteRuleTokenStream stream_ID=new RewriteRuleTokenStream(adaptor,"token ID");
        RewriteRuleTokenStream stream_38=new RewriteRuleTokenStream(adaptor,"token 38");
        RewriteRuleSubtreeStream stream_expression=new RewriteRuleSubtreeStream(adaptor,"rule expression");
        RewriteRuleSubtreeStream stream_valueExpression=new RewriteRuleSubtreeStream(adaptor,"rule valueExpression");
        RewriteRuleSubtreeStream stream_generalPathExpression=new RewriteRuleSubtreeStream(adaptor,"rule generalPathExpression");
        try {
            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:292:2: ( ( ( ID ':' )=> ID ':' expression ->) | ( ( '\"' ID '\"' ':' )=> '\"' ID '\"' ':' expression ->) | ( VAR '.' STAR )=> VAR '.' STAR ->| ( VAR )=>p= generalPathExpression ( ( ':' )=> ':' e2= expression ->| ->) |v= valueExpression ':' e2= expression ->)
            int alt45=5;
            int LA45_0 = input.LA(1);

            if ( (LA45_0==ID) ) {
                int LA45_1 = input.LA(2);

                if ( (synpred22_Meteor()) ) {
                    alt45=1;
                }
                else if ( (synpred25_Meteor()) ) {
                    alt45=4;
                }
                else if ( (true) ) {
                    alt45=5;
                }
                else {
                    if (state.backtracking>0) {state.failed=true; return retval;}
                    NoViableAltException nvae =
                        new NoViableAltException("", 45, 1, input);

                    throw nvae;

                }
            }
            else if ( (LA45_0==38) && (synpred23_Meteor())) {
                alt45=2;
            }
            else if ( (LA45_0==VAR) ) {
                int LA45_3 = input.LA(2);

                if ( (synpred24_Meteor()) ) {
                    alt45=3;
                }
                else if ( (synpred25_Meteor()) ) {
                    alt45=4;
                }
                else if ( (true) ) {
                    alt45=5;
                }
                else {
                    if (state.backtracking>0) {state.failed=true; return retval;}
                    NoViableAltException nvae =
                        new NoViableAltException("", 45, 3, input);

                    throw nvae;

                }
            }
            else if ( (LA45_0==40) ) {
                int LA45_4 = input.LA(2);

                if ( (synpred25_Meteor()) ) {
                    alt45=4;
                }
                else if ( (true) ) {
                    alt45=5;
                }
                else {
                    if (state.backtracking>0) {state.failed=true; return retval;}
                    NoViableAltException nvae =
                        new NoViableAltException("", 45, 4, input);

                    throw nvae;

                }
            }
            else if ( (LA45_0==FN) ) {
                int LA45_5 = input.LA(2);

                if ( (synpred25_Meteor()) ) {
                    alt45=4;
                }
                else if ( (true) ) {
                    alt45=5;
                }
                else {
                    if (state.backtracking>0) {state.failed=true; return retval;}
                    NoViableAltException nvae =
                        new NoViableAltException("", 45, 5, input);

                    throw nvae;

                }
            }
            else if ( (LA45_0==41) ) {
                int LA45_6 = input.LA(2);

                if ( (synpred25_Meteor()) ) {
                    alt45=4;
                }
                else if ( (true) ) {
                    alt45=5;
                }
                else {
                    if (state.backtracking>0) {state.failed=true; return retval;}
                    NoViableAltException nvae =
                        new NoViableAltException("", 45, 6, input);

                    throw nvae;

                }
            }
            else if ( (LA45_0==64) ) {
                int LA45_7 = input.LA(2);

                if ( (synpred25_Meteor()) ) {
                    alt45=4;
                }
                else if ( (true) ) {
                    alt45=5;
                }
                else {
                    if (state.backtracking>0) {state.failed=true; return retval;}
                    NoViableAltException nvae =
                        new NoViableAltException("", 45, 7, input);

                    throw nvae;

                }
            }
            else if ( (LA45_0==61) ) {
                int LA45_8 = input.LA(2);

                if ( (synpred25_Meteor()) ) {
                    alt45=4;
                }
                else if ( (true) ) {
                    alt45=5;
                }
                else {
                    if (state.backtracking>0) {state.failed=true; return retval;}
                    NoViableAltException nvae =
                        new NoViableAltException("", 45, 8, input);

                    throw nvae;

                }
            }
            else if ( (LA45_0==DECIMAL) ) {
                int LA45_9 = input.LA(2);

                if ( (synpred25_Meteor()) ) {
                    alt45=4;
                }
                else if ( (true) ) {
                    alt45=5;
                }
                else {
                    if (state.backtracking>0) {state.failed=true; return retval;}
                    NoViableAltException nvae =
                        new NoViableAltException("", 45, 9, input);

                    throw nvae;

                }
            }
            else if ( (LA45_0==STRING) ) {
                int LA45_10 = input.LA(2);

                if ( (synpred25_Meteor()) ) {
                    alt45=4;
                }
                else if ( (true) ) {
                    alt45=5;
                }
                else {
                    if (state.backtracking>0) {state.failed=true; return retval;}
                    NoViableAltException nvae =
                        new NoViableAltException("", 45, 10, input);

                    throw nvae;

                }
            }
            else if ( (LA45_0==UINT) ) {
                int LA45_11 = input.LA(2);

                if ( (synpred25_Meteor()) ) {
                    alt45=4;
                }
                else if ( (true) ) {
                    alt45=5;
                }
                else {
                    if (state.backtracking>0) {state.failed=true; return retval;}
                    NoViableAltException nvae =
                        new NoViableAltException("", 45, 11, input);

                    throw nvae;

                }
            }
            else if ( (LA45_0==INTEGER) ) {
                int LA45_12 = input.LA(2);

                if ( (synpred25_Meteor()) ) {
                    alt45=4;
                }
                else if ( (true) ) {
                    alt45=5;
                }
                else {
                    if (state.backtracking>0) {state.failed=true; return retval;}
                    NoViableAltException nvae =
                        new NoViableAltException("", 45, 12, input);

                    throw nvae;

                }
            }
            else if ( (LA45_0==62) ) {
                int LA45_13 = input.LA(2);

                if ( (synpred25_Meteor()) ) {
                    alt45=4;
                }
                else if ( (true) ) {
                    alt45=5;
                }
                else {
                    if (state.backtracking>0) {state.failed=true; return retval;}
                    NoViableAltException nvae =
                        new NoViableAltException("", 45, 13, input);

                    throw nvae;

                }
            }
            else if ( (LA45_0==59) ) {
                int LA45_14 = input.LA(2);

                if ( (synpred25_Meteor()) ) {
                    alt45=4;
                }
                else if ( (true) ) {
                    alt45=5;
                }
                else {
                    if (state.backtracking>0) {state.failed=true; return retval;}
                    NoViableAltException nvae =
                        new NoViableAltException("", 45, 14, input);

                    throw nvae;

                }
            }
            else if ( (LA45_0==67) ) {
                int LA45_15 = input.LA(2);

                if ( (synpred25_Meteor()) ) {
                    alt45=4;
                }
                else if ( (true) ) {
                    alt45=5;
                }
                else {
                    if (state.backtracking>0) {state.failed=true; return retval;}
                    NoViableAltException nvae =
                        new NoViableAltException("", 45, 15, input);

                    throw nvae;

                }
            }
            else {
                if (state.backtracking>0) {state.failed=true; return retval;}
                NoViableAltException nvae =
                    new NoViableAltException("", 45, 0, input);

                throw nvae;

            }
            switch (alt45) {
                case 1 :
                    // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:292:4: ( ( ID ':' )=> ID ':' expression ->)
                    {
                    // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:292:4: ( ( ID ':' )=> ID ':' expression ->)
                    // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:292:5: ( ID ':' )=> ID ':' expression
                    {
                    ID82=(Token)match(input,ID,FOLLOW_ID_in_fieldAssignment1876); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_ID.add(ID82);


                    char_literal83=(Token)match(input,49,FOLLOW_49_in_fieldAssignment1878); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_49.add(char_literal83);


                    pushFollow(FOLLOW_expression_in_fieldAssignment1880);
                    expression84=expression();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_expression.add(expression84.getTree());

                    if ( state.backtracking==0 ) { ((objectCreation_scope)objectCreation_stack.peek()).mappings.add(new ObjectCreation.FieldAssignment((ID82!=null?ID82.getText():null), (expression84!=null?((EvaluationExpression)expression84.tree):null))); }

                    // AST REWRITE
                    // elements: 
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    if ( state.backtracking==0 ) {

                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (EvaluationExpression)adaptor.nil();
                    // 293:104: ->
                    {
                        root_0 = null;
                    }


                    retval.tree = root_0;
                    }

                    }


                    }
                    break;
                case 2 :
                    // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:294:5: ( ( '\"' ID '\"' ':' )=> '\"' ID '\"' ':' expression ->)
                    {
                    // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:294:5: ( ( '\"' ID '\"' ':' )=> '\"' ID '\"' ':' expression ->)
                    // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:294:6: ( '\"' ID '\"' ':' )=> '\"' ID '\"' ':' expression
                    {
                    char_literal85=(Token)match(input,38,FOLLOW_38_in_fieldAssignment1909); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_38.add(char_literal85);


                    ID86=(Token)match(input,ID,FOLLOW_ID_in_fieldAssignment1911); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_ID.add(ID86);


                    char_literal87=(Token)match(input,38,FOLLOW_38_in_fieldAssignment1913); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_38.add(char_literal87);


                    char_literal88=(Token)match(input,49,FOLLOW_49_in_fieldAssignment1915); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_49.add(char_literal88);


                    pushFollow(FOLLOW_expression_in_fieldAssignment1917);
                    expression89=expression();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_expression.add(expression89.getTree());

                    if ( state.backtracking==0 ) { ((objectCreation_scope)objectCreation_stack.peek()).mappings.add(new ObjectCreation.FieldAssignment((ID86!=null?ID86.getText():null), (expression89!=null?((EvaluationExpression)expression89.tree):null))); }

                    // AST REWRITE
                    // elements: 
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    if ( state.backtracking==0 ) {

                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (EvaluationExpression)adaptor.nil();
                    // 295:104: ->
                    {
                        root_0 = null;
                    }


                    retval.tree = root_0;
                    }

                    }


                    }
                    break;
                case 3 :
                    // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:296:5: ( VAR '.' STAR )=> VAR '.' STAR
                    {
                    VAR90=(Token)match(input,VAR,FOLLOW_VAR_in_fieldAssignment1943); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_VAR.add(VAR90);


                    char_literal91=(Token)match(input,48,FOLLOW_48_in_fieldAssignment1945); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_48.add(char_literal91);


                    STAR92=(Token)match(input,STAR,FOLLOW_STAR_in_fieldAssignment1947); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_STAR.add(STAR92);


                    if ( state.backtracking==0 ) { ((objectCreation_scope)objectCreation_stack.peek()).mappings.add(new ObjectCreation.CopyFields(getInputSelection(VAR90))); }

                    // AST REWRITE
                    // elements: 
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    if ( state.backtracking==0 ) {

                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (EvaluationExpression)adaptor.nil();
                    // 296:126: ->
                    {
                        root_0 = null;
                    }


                    retval.tree = root_0;
                    }

                    }
                    break;
                case 4 :
                    // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:297:5: ( VAR )=>p= generalPathExpression ( ( ':' )=> ':' e2= expression ->| ->)
                    {
                    pushFollow(FOLLOW_generalPathExpression_in_fieldAssignment1964);
                    p=generalPathExpression();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_generalPathExpression.add(p.getTree());

                    // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:297:37: ( ( ':' )=> ':' e2= expression ->| ->)
                    int alt44=2;
                    int LA44_0 = input.LA(1);

                    if ( (LA44_0==49) && (synpred26_Meteor())) {
                        alt44=1;
                    }
                    else if ( (LA44_0==45||LA44_0==69) ) {
                        alt44=2;
                    }
                    else {
                        if (state.backtracking>0) {state.failed=true; return retval;}
                        NoViableAltException nvae =
                            new NoViableAltException("", 44, 0, input);

                        throw nvae;

                    }
                    switch (alt44) {
                        case 1 :
                            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:298:5: ( ':' )=> ':' e2= expression
                            {
                            char_literal93=(Token)match(input,49,FOLLOW_49_in_fieldAssignment1977); if (state.failed) return retval; 
                            if ( state.backtracking==0 ) stream_49.add(char_literal93);


                            pushFollow(FOLLOW_expression_in_fieldAssignment1981);
                            e2=expression();

                            state._fsp--;
                            if (state.failed) return retval;
                            if ( state.backtracking==0 ) stream_expression.add(e2.getTree());

                            if ( state.backtracking==0 ) { ((objectCreation_scope)objectCreation_stack.peek()).mappings.add(new ObjectCreation.SymbolicAssignment((p!=null?((EvaluationExpression)p.tree):null), (e2!=null?((EvaluationExpression)e2.tree):null))); }

                            // AST REWRITE
                            // elements: 
                            // token labels: 
                            // rule labels: retval
                            // token list labels: 
                            // rule list labels: 
                            // wildcard labels: 
                            if ( state.backtracking==0 ) {

                            retval.tree = root_0;
                            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                            root_0 = (EvaluationExpression)adaptor.nil();
                            // 298:124: ->
                            {
                                root_0 = null;
                            }


                            retval.tree = root_0;
                            }

                            }
                            break;
                        case 2 :
                            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:299:21: 
                            {
                            if ( state.backtracking==0 ) { ((objectCreation_scope)objectCreation_stack.peek()).mappings.add(new ObjectCreation.FieldAssignment(getAssignmentName((p!=null?((EvaluationExpression)p.tree):null)), (p!=null?((EvaluationExpression)p.tree):null))); }

                            // AST REWRITE
                            // elements: 
                            // token labels: 
                            // rule labels: retval
                            // token list labels: 
                            // rule list labels: 
                            // wildcard labels: 
                            if ( state.backtracking==0 ) {

                            retval.tree = root_0;
                            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                            root_0 = (EvaluationExpression)adaptor.nil();
                            // 299:129: ->
                            {
                                root_0 = null;
                            }


                            retval.tree = root_0;
                            }

                            }
                            break;

                    }


                    }
                    break;
                case 5 :
                    // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:301:5: v= valueExpression ':' e2= expression
                    {
                    pushFollow(FOLLOW_valueExpression_in_fieldAssignment2011);
                    v=valueExpression();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_valueExpression.add(v.getTree());

                    char_literal94=(Token)match(input,49,FOLLOW_49_in_fieldAssignment2013); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_49.add(char_literal94);


                    pushFollow(FOLLOW_expression_in_fieldAssignment2017);
                    e2=expression();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_expression.add(e2.getTree());

                    if ( state.backtracking==0 ) { ((objectCreation_scope)objectCreation_stack.peek()).mappings.add(new ObjectCreation.SymbolicAssignment((v!=null?((EvaluationExpression)v.tree):null), (e2!=null?((EvaluationExpression)e2.tree):null))); }

                    // AST REWRITE
                    // elements: 
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    if ( state.backtracking==0 ) {

                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (EvaluationExpression)adaptor.nil();
                    // 301:134: ->
                    {
                        root_0 = null;
                    }


                    retval.tree = root_0;
                    }

                    }
                    break;

            }
            retval.stop = input.LT(-1);


            if ( state.backtracking==0 ) {

            retval.tree = (EvaluationExpression)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
        catch (RecognitionException re) {
             explainUsage("inside of a json object {...} only <field: expression>, <$var.path>, <$var = operator> or <$var: expression> are allowed", re); 
        }

        finally {
        	// do for sure before leaving
        }
        return retval;
    }
    // $ANTLR end "fieldAssignment"


    protected static class objectCreation_scope {
        List<ObjectCreation.Mapping> mappings;
    }
    protected Stack objectCreation_stack = new Stack();


    public static class objectCreation_return extends ParserRuleReturnScope {
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };


    // $ANTLR start "objectCreation"
    // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:305:1: objectCreation : '{' ( fieldAssignment ( ',' fieldAssignment )* ( ',' )? )? '}' -> ^( EXPRESSION[\"ObjectCreation\"] ) ;
    public final MeteorParser.objectCreation_return objectCreation() throws RecognitionException {
        objectCreation_stack.push(new objectCreation_scope());
        MeteorParser.objectCreation_return retval = new MeteorParser.objectCreation_return();
        retval.start = input.LT(1);


        EvaluationExpression root_0 = null;

        Token char_literal95=null;
        Token char_literal97=null;
        Token char_literal99=null;
        Token char_literal100=null;
        MeteorParser.fieldAssignment_return fieldAssignment96 =null;

        MeteorParser.fieldAssignment_return fieldAssignment98 =null;


        EvaluationExpression char_literal95_tree=null;
        EvaluationExpression char_literal97_tree=null;
        EvaluationExpression char_literal99_tree=null;
        EvaluationExpression char_literal100_tree=null;
        RewriteRuleTokenStream stream_67=new RewriteRuleTokenStream(adaptor,"token 67");
        RewriteRuleTokenStream stream_69=new RewriteRuleTokenStream(adaptor,"token 69");
        RewriteRuleTokenStream stream_45=new RewriteRuleTokenStream(adaptor,"token 45");
        RewriteRuleSubtreeStream stream_fieldAssignment=new RewriteRuleSubtreeStream(adaptor,"rule fieldAssignment");
         ((objectCreation_scope)objectCreation_stack.peek()).mappings = new ArrayList<ObjectCreation.Mapping>(); 
                paraphrase.push("a json object"); 
        try {
            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:310:2: ( '{' ( fieldAssignment ( ',' fieldAssignment )* ( ',' )? )? '}' -> ^( EXPRESSION[\"ObjectCreation\"] ) )
            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:310:4: '{' ( fieldAssignment ( ',' fieldAssignment )* ( ',' )? )? '}'
            {
            char_literal95=(Token)match(input,67,FOLLOW_67_in_objectCreation2055); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_67.add(char_literal95);


            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:310:8: ( fieldAssignment ( ',' fieldAssignment )* ( ',' )? )?
            int alt48=2;
            int LA48_0 = input.LA(1);

            if ( (LA48_0==DECIMAL||LA48_0==FN||LA48_0==ID||LA48_0==INTEGER||(LA48_0 >= STRING && LA48_0 <= UINT)||LA48_0==VAR||LA48_0==38||(LA48_0 >= 40 && LA48_0 <= 41)||LA48_0==59||(LA48_0 >= 61 && LA48_0 <= 62)||LA48_0==64||LA48_0==67) ) {
                alt48=1;
            }
            switch (alt48) {
                case 1 :
                    // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:310:9: fieldAssignment ( ',' fieldAssignment )* ( ',' )?
                    {
                    pushFollow(FOLLOW_fieldAssignment_in_objectCreation2058);
                    fieldAssignment96=fieldAssignment();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_fieldAssignment.add(fieldAssignment96.getTree());

                    // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:310:25: ( ',' fieldAssignment )*
                    loop46:
                    do {
                        int alt46=2;
                        int LA46_0 = input.LA(1);

                        if ( (LA46_0==45) ) {
                            int LA46_1 = input.LA(2);

                            if ( (LA46_1==DECIMAL||LA46_1==FN||LA46_1==ID||LA46_1==INTEGER||(LA46_1 >= STRING && LA46_1 <= UINT)||LA46_1==VAR||LA46_1==38||(LA46_1 >= 40 && LA46_1 <= 41)||LA46_1==59||(LA46_1 >= 61 && LA46_1 <= 62)||LA46_1==64||LA46_1==67) ) {
                                alt46=1;
                            }


                        }


                        switch (alt46) {
                    	case 1 :
                    	    // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:310:26: ',' fieldAssignment
                    	    {
                    	    char_literal97=(Token)match(input,45,FOLLOW_45_in_objectCreation2061); if (state.failed) return retval; 
                    	    if ( state.backtracking==0 ) stream_45.add(char_literal97);


                    	    pushFollow(FOLLOW_fieldAssignment_in_objectCreation2063);
                    	    fieldAssignment98=fieldAssignment();

                    	    state._fsp--;
                    	    if (state.failed) return retval;
                    	    if ( state.backtracking==0 ) stream_fieldAssignment.add(fieldAssignment98.getTree());

                    	    }
                    	    break;

                    	default :
                    	    break loop46;
                        }
                    } while (true);


                    // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:310:48: ( ',' )?
                    int alt47=2;
                    int LA47_0 = input.LA(1);

                    if ( (LA47_0==45) ) {
                        alt47=1;
                    }
                    switch (alt47) {
                        case 1 :
                            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:310:48: ','
                            {
                            char_literal99=(Token)match(input,45,FOLLOW_45_in_objectCreation2067); if (state.failed) return retval; 
                            if ( state.backtracking==0 ) stream_45.add(char_literal99);


                            }
                            break;

                    }


                    }
                    break;

            }


            char_literal100=(Token)match(input,69,FOLLOW_69_in_objectCreation2072); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_69.add(char_literal100);


            // AST REWRITE
            // elements: 
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            if ( state.backtracking==0 ) {

            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (EvaluationExpression)adaptor.nil();
            // 310:59: -> ^( EXPRESSION[\"ObjectCreation\"] )
            {
                // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:310:62: ^( EXPRESSION[\"ObjectCreation\"] )
                {
                EvaluationExpression root_1 = (EvaluationExpression)adaptor.nil();
                root_1 = (EvaluationExpression)adaptor.becomeRoot(
                (EvaluationExpression)adaptor.create(EXPRESSION, "ObjectCreation")
                , root_1);

                adaptor.addChild(root_1,  ((objectCreation_scope)objectCreation_stack.peek()).mappings );

                adaptor.addChild(root_0, root_1);
                }

            }


            retval.tree = root_0;
            }

            }

            retval.stop = input.LT(-1);


            if ( state.backtracking==0 ) {

            retval.tree = (EvaluationExpression)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
            if ( state.backtracking==0 ) { paraphrase.pop(); }
        }
        catch (MissingTokenException re) {
             explainUsage("expected <,> or <}> after a complete field assignment inside of a json object", re); 
        }

        finally {
        	// do for sure before leaving
            objectCreation_stack.pop();
        }
        return retval;
    }
    // $ANTLR end "objectCreation"


    public static class literal_return extends ParserRuleReturnScope {
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };


    // $ANTLR start "literal"
    // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:313:1: literal : (val= 'true' -> ^( EXPRESSION[\"ConstantExpression\"] ) |val= 'false' -> ^( EXPRESSION[\"ConstantExpression\"] ) |val= DECIMAL -> ^( EXPRESSION[\"ConstantExpression\"] ) |val= STRING -> ^( EXPRESSION[\"ConstantExpression\"] ) | (val= UINT |val= INTEGER ) -> ^( EXPRESSION[\"ConstantExpression\"] ) | 'null' ->);
    public final MeteorParser.literal_return literal() throws RecognitionException {
        MeteorParser.literal_return retval = new MeteorParser.literal_return();
        retval.start = input.LT(1);


        EvaluationExpression root_0 = null;

        Token val=null;
        Token string_literal101=null;

        EvaluationExpression val_tree=null;
        EvaluationExpression string_literal101_tree=null;
        RewriteRuleTokenStream stream_INTEGER=new RewriteRuleTokenStream(adaptor,"token INTEGER");
        RewriteRuleTokenStream stream_DECIMAL=new RewriteRuleTokenStream(adaptor,"token DECIMAL");
        RewriteRuleTokenStream stream_64=new RewriteRuleTokenStream(adaptor,"token 64");
        RewriteRuleTokenStream stream_62=new RewriteRuleTokenStream(adaptor,"token 62");
        RewriteRuleTokenStream stream_UINT=new RewriteRuleTokenStream(adaptor,"token UINT");
        RewriteRuleTokenStream stream_61=new RewriteRuleTokenStream(adaptor,"token 61");
        RewriteRuleTokenStream stream_STRING=new RewriteRuleTokenStream(adaptor,"token STRING");

         paraphrase.push("a literal"); 
        try {
            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:316:2: (val= 'true' -> ^( EXPRESSION[\"ConstantExpression\"] ) |val= 'false' -> ^( EXPRESSION[\"ConstantExpression\"] ) |val= DECIMAL -> ^( EXPRESSION[\"ConstantExpression\"] ) |val= STRING -> ^( EXPRESSION[\"ConstantExpression\"] ) | (val= UINT |val= INTEGER ) -> ^( EXPRESSION[\"ConstantExpression\"] ) | 'null' ->)
            int alt50=6;
            switch ( input.LA(1) ) {
            case 64:
                {
                alt50=1;
                }
                break;
            case 61:
                {
                alt50=2;
                }
                break;
            case DECIMAL:
                {
                alt50=3;
                }
                break;
            case STRING:
                {
                alt50=4;
                }
                break;
            case INTEGER:
            case UINT:
                {
                alt50=5;
                }
                break;
            case 62:
                {
                alt50=6;
                }
                break;
            default:
                if (state.backtracking>0) {state.failed=true; return retval;}
                NoViableAltException nvae =
                    new NoViableAltException("", 50, 0, input);

                throw nvae;

            }

            switch (alt50) {
                case 1 :
                    // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:316:4: val= 'true'
                    {
                    val=(Token)match(input,64,FOLLOW_64_in_literal2110); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_64.add(val);


                    // AST REWRITE
                    // elements: 
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    if ( state.backtracking==0 ) {

                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (EvaluationExpression)adaptor.nil();
                    // 316:15: -> ^( EXPRESSION[\"ConstantExpression\"] )
                    {
                        // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:316:18: ^( EXPRESSION[\"ConstantExpression\"] )
                        {
                        EvaluationExpression root_1 = (EvaluationExpression)adaptor.nil();
                        root_1 = (EvaluationExpression)adaptor.becomeRoot(
                        (EvaluationExpression)adaptor.create(EXPRESSION, "ConstantExpression")
                        , root_1);

                        adaptor.addChild(root_1,  Boolean.TRUE );

                        adaptor.addChild(root_0, root_1);
                        }

                    }


                    retval.tree = root_0;
                    }

                    }
                    break;
                case 2 :
                    // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:317:4: val= 'false'
                    {
                    val=(Token)match(input,61,FOLLOW_61_in_literal2126); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_61.add(val);


                    // AST REWRITE
                    // elements: 
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    if ( state.backtracking==0 ) {

                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (EvaluationExpression)adaptor.nil();
                    // 317:16: -> ^( EXPRESSION[\"ConstantExpression\"] )
                    {
                        // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:317:19: ^( EXPRESSION[\"ConstantExpression\"] )
                        {
                        EvaluationExpression root_1 = (EvaluationExpression)adaptor.nil();
                        root_1 = (EvaluationExpression)adaptor.becomeRoot(
                        (EvaluationExpression)adaptor.create(EXPRESSION, "ConstantExpression")
                        , root_1);

                        adaptor.addChild(root_1,  Boolean.FALSE );

                        adaptor.addChild(root_0, root_1);
                        }

                    }


                    retval.tree = root_0;
                    }

                    }
                    break;
                case 3 :
                    // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:318:4: val= DECIMAL
                    {
                    val=(Token)match(input,DECIMAL,FOLLOW_DECIMAL_in_literal2142); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_DECIMAL.add(val);


                    // AST REWRITE
                    // elements: 
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    if ( state.backtracking==0 ) {

                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (EvaluationExpression)adaptor.nil();
                    // 318:16: -> ^( EXPRESSION[\"ConstantExpression\"] )
                    {
                        // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:318:19: ^( EXPRESSION[\"ConstantExpression\"] )
                        {
                        EvaluationExpression root_1 = (EvaluationExpression)adaptor.nil();
                        root_1 = (EvaluationExpression)adaptor.becomeRoot(
                        (EvaluationExpression)adaptor.create(EXPRESSION, "ConstantExpression")
                        , root_1);

                        adaptor.addChild(root_1,  new BigDecimal((val!=null?val.getText():null)) );

                        adaptor.addChild(root_0, root_1);
                        }

                    }


                    retval.tree = root_0;
                    }

                    }
                    break;
                case 4 :
                    // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:319:4: val= STRING
                    {
                    val=(Token)match(input,STRING,FOLLOW_STRING_in_literal2158); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_STRING.add(val);


                    // AST REWRITE
                    // elements: 
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    if ( state.backtracking==0 ) {

                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (EvaluationExpression)adaptor.nil();
                    // 319:15: -> ^( EXPRESSION[\"ConstantExpression\"] )
                    {
                        // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:319:18: ^( EXPRESSION[\"ConstantExpression\"] )
                        {
                        EvaluationExpression root_1 = (EvaluationExpression)adaptor.nil();
                        root_1 = (EvaluationExpression)adaptor.becomeRoot(
                        (EvaluationExpression)adaptor.create(EXPRESSION, "ConstantExpression")
                        , root_1);

                        adaptor.addChild(root_1,  val.getText() );

                        adaptor.addChild(root_0, root_1);
                        }

                    }


                    retval.tree = root_0;
                    }

                    }
                    break;
                case 5 :
                    // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:320:5: (val= UINT |val= INTEGER )
                    {
                    // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:320:5: (val= UINT |val= INTEGER )
                    int alt49=2;
                    int LA49_0 = input.LA(1);

                    if ( (LA49_0==UINT) ) {
                        alt49=1;
                    }
                    else if ( (LA49_0==INTEGER) ) {
                        alt49=2;
                    }
                    else {
                        if (state.backtracking>0) {state.failed=true; return retval;}
                        NoViableAltException nvae =
                            new NoViableAltException("", 49, 0, input);

                        throw nvae;

                    }
                    switch (alt49) {
                        case 1 :
                            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:320:6: val= UINT
                            {
                            val=(Token)match(input,UINT,FOLLOW_UINT_in_literal2176); if (state.failed) return retval; 
                            if ( state.backtracking==0 ) stream_UINT.add(val);


                            }
                            break;
                        case 2 :
                            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:320:17: val= INTEGER
                            {
                            val=(Token)match(input,INTEGER,FOLLOW_INTEGER_in_literal2182); if (state.failed) return retval; 
                            if ( state.backtracking==0 ) stream_INTEGER.add(val);


                            }
                            break;

                    }


                    // AST REWRITE
                    // elements: 
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    if ( state.backtracking==0 ) {

                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (EvaluationExpression)adaptor.nil();
                    // 320:30: -> ^( EXPRESSION[\"ConstantExpression\"] )
                    {
                        // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:320:33: ^( EXPRESSION[\"ConstantExpression\"] )
                        {
                        EvaluationExpression root_1 = (EvaluationExpression)adaptor.nil();
                        root_1 = (EvaluationExpression)adaptor.becomeRoot(
                        (EvaluationExpression)adaptor.create(EXPRESSION, "ConstantExpression")
                        , root_1);

                        adaptor.addChild(root_1,  parseInt((val!=null?val.getText():null)) );

                        adaptor.addChild(root_0, root_1);
                        }

                    }


                    retval.tree = root_0;
                    }

                    }
                    break;
                case 6 :
                    // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:321:5: 'null'
                    {
                    string_literal101=(Token)match(input,62,FOLLOW_62_in_literal2198); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_62.add(string_literal101);


                    // AST REWRITE
                    // elements: 
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    if ( state.backtracking==0 ) {

                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (EvaluationExpression)adaptor.nil();
                    // 321:12: ->
                    {
                        adaptor.addChild(root_0,  ConstantExpression.NULL );

                    }


                    retval.tree = root_0;
                    }

                    }
                    break;

            }
            retval.stop = input.LT(-1);


            if ( state.backtracking==0 ) {

            retval.tree = (EvaluationExpression)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
            if ( state.backtracking==0 ) { paraphrase.pop(); }
        }

        catch (RecognitionException e) {
          throw e;
        }

        finally {
        	// do for sure before leaving
        }
        return retval;
    }
    // $ANTLR end "literal"


    public static class arrayCreation_return extends ParserRuleReturnScope {
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };


    // $ANTLR start "arrayCreation"
    // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:323:1: arrayCreation : '[' (elems+= expression ( ',' elems+= expression )* ( ',' )? )? ']' -> ^( EXPRESSION[\"ArrayCreation\"] ) ;
    public final MeteorParser.arrayCreation_return arrayCreation() throws RecognitionException {
        MeteorParser.arrayCreation_return retval = new MeteorParser.arrayCreation_return();
        retval.start = input.LT(1);


        EvaluationExpression root_0 = null;

        Token char_literal102=null;
        Token char_literal103=null;
        Token char_literal104=null;
        Token char_literal105=null;
        List list_elems=null;
        RuleReturnScope elems = null;
        EvaluationExpression char_literal102_tree=null;
        EvaluationExpression char_literal103_tree=null;
        EvaluationExpression char_literal104_tree=null;
        EvaluationExpression char_literal105_tree=null;
        RewriteRuleTokenStream stream_59=new RewriteRuleTokenStream(adaptor,"token 59");
        RewriteRuleTokenStream stream_45=new RewriteRuleTokenStream(adaptor,"token 45");
        RewriteRuleTokenStream stream_60=new RewriteRuleTokenStream(adaptor,"token 60");
        RewriteRuleSubtreeStream stream_expression=new RewriteRuleSubtreeStream(adaptor,"rule expression");
         paraphrase.push("a json array"); 
        try {
            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:326:2: ( '[' (elems+= expression ( ',' elems+= expression )* ( ',' )? )? ']' -> ^( EXPRESSION[\"ArrayCreation\"] ) )
            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:326:5: '[' (elems+= expression ( ',' elems+= expression )* ( ',' )? )? ']'
            {
            char_literal102=(Token)match(input,59,FOLLOW_59_in_arrayCreation2222); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_59.add(char_literal102);


            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:326:9: (elems+= expression ( ',' elems+= expression )* ( ',' )? )?
            int alt53=2;
            int LA53_0 = input.LA(1);

            if ( (LA53_0==DECIMAL||LA53_0==FN||LA53_0==ID||LA53_0==INTEGER||(LA53_0 >= STRING && LA53_0 <= UINT)||LA53_0==VAR||LA53_0==36||(LA53_0 >= 40 && LA53_0 <= 41)||LA53_0==44||LA53_0==47||LA53_0==59||(LA53_0 >= 61 && LA53_0 <= 64)||(LA53_0 >= 66 && LA53_0 <= 67)||LA53_0==70) ) {
                alt53=1;
            }
            switch (alt53) {
                case 1 :
                    // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:326:10: elems+= expression ( ',' elems+= expression )* ( ',' )?
                    {
                    pushFollow(FOLLOW_expression_in_arrayCreation2227);
                    elems=expression();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_expression.add(elems.getTree());
                    if (list_elems==null) list_elems=new ArrayList();
                    list_elems.add(elems.getTree());


                    // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:326:28: ( ',' elems+= expression )*
                    loop51:
                    do {
                        int alt51=2;
                        int LA51_0 = input.LA(1);

                        if ( (LA51_0==45) ) {
                            int LA51_1 = input.LA(2);

                            if ( (LA51_1==DECIMAL||LA51_1==FN||LA51_1==ID||LA51_1==INTEGER||(LA51_1 >= STRING && LA51_1 <= UINT)||LA51_1==VAR||LA51_1==36||(LA51_1 >= 40 && LA51_1 <= 41)||LA51_1==44||LA51_1==47||LA51_1==59||(LA51_1 >= 61 && LA51_1 <= 64)||(LA51_1 >= 66 && LA51_1 <= 67)||LA51_1==70) ) {
                                alt51=1;
                            }


                        }


                        switch (alt51) {
                    	case 1 :
                    	    // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:326:29: ',' elems+= expression
                    	    {
                    	    char_literal103=(Token)match(input,45,FOLLOW_45_in_arrayCreation2230); if (state.failed) return retval; 
                    	    if ( state.backtracking==0 ) stream_45.add(char_literal103);


                    	    pushFollow(FOLLOW_expression_in_arrayCreation2234);
                    	    elems=expression();

                    	    state._fsp--;
                    	    if (state.failed) return retval;
                    	    if ( state.backtracking==0 ) stream_expression.add(elems.getTree());
                    	    if (list_elems==null) list_elems=new ArrayList();
                    	    list_elems.add(elems.getTree());


                    	    }
                    	    break;

                    	default :
                    	    break loop51;
                        }
                    } while (true);


                    // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:326:53: ( ',' )?
                    int alt52=2;
                    int LA52_0 = input.LA(1);

                    if ( (LA52_0==45) ) {
                        alt52=1;
                    }
                    switch (alt52) {
                        case 1 :
                            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:326:53: ','
                            {
                            char_literal104=(Token)match(input,45,FOLLOW_45_in_arrayCreation2238); if (state.failed) return retval; 
                            if ( state.backtracking==0 ) stream_45.add(char_literal104);


                            }
                            break;

                    }


                    }
                    break;

            }


            char_literal105=(Token)match(input,60,FOLLOW_60_in_arrayCreation2243); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_60.add(char_literal105);


            // AST REWRITE
            // elements: 
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            if ( state.backtracking==0 ) {

            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (EvaluationExpression)adaptor.nil();
            // 326:64: -> ^( EXPRESSION[\"ArrayCreation\"] )
            {
                // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:326:67: ^( EXPRESSION[\"ArrayCreation\"] )
                {
                EvaluationExpression root_1 = (EvaluationExpression)adaptor.nil();
                root_1 = (EvaluationExpression)adaptor.becomeRoot(
                (EvaluationExpression)adaptor.create(EXPRESSION, "ArrayCreation")
                , root_1);

                adaptor.addChild(root_1,  list_elems == null ? new EvaluationExpression[0] : list_elems.toArray(new EvaluationExpression[list_elems.size()]) );

                adaptor.addChild(root_0, root_1);
                }

            }


            retval.tree = root_0;
            }

            }

            retval.stop = input.LT(-1);


            if ( state.backtracking==0 ) {

            retval.tree = (EvaluationExpression)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
            if ( state.backtracking==0 ) { paraphrase.pop(); }
        }

        catch (RecognitionException e) {
          throw e;
        }

        finally {
        	// do for sure before leaving
        }
        return retval;
    }
    // $ANTLR end "arrayCreation"


    protected static class operator_scope {
        int numInputs;
        Operator<?> result;
    }
    protected Stack operator_stack = new Stack();


    public static class operator_return extends ParserRuleReturnScope {
        public Operator<?> op=null;
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };


    // $ANTLR start "operator"
    // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:332:1: operator returns [Operator<?> op=null] : ( readOperator | writeOperator | genericOperator );
    public final MeteorParser.operator_return operator() throws RecognitionException {
        operator_stack.push(new operator_scope());
        MeteorParser.operator_return retval = new MeteorParser.operator_return();
        retval.start = input.LT(1);


        EvaluationExpression root_0 = null;

        MeteorParser.readOperator_return readOperator106 =null;

        MeteorParser.writeOperator_return writeOperator107 =null;

        MeteorParser.genericOperator_return genericOperator108 =null;



        try {
            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:336:2: ( readOperator | writeOperator | genericOperator )
            int alt54=3;
            switch ( input.LA(1) ) {
            case VAR:
                {
                int LA54_1 = input.LA(2);

                if ( (LA54_1==53) ) {
                    int LA54_5 = input.LA(3);

                    if ( (LA54_5==63) ) {
                        alt54=1;
                    }
                    else if ( (LA54_5==ID) ) {
                        alt54=3;
                    }
                    else {
                        if (state.backtracking>0) {state.failed=true; return retval;}
                        NoViableAltException nvae =
                            new NoViableAltException("", 54, 5, input);

                        throw nvae;

                    }
                }
                else if ( (LA54_1==45) ) {
                    alt54=3;
                }
                else {
                    if (state.backtracking>0) {state.failed=true; return retval;}
                    NoViableAltException nvae =
                        new NoViableAltException("", 54, 1, input);

                    throw nvae;

                }
                }
                break;
            case 63:
                {
                alt54=1;
                }
                break;
            case 66:
                {
                alt54=2;
                }
                break;
            case ID:
                {
                alt54=3;
                }
                break;
            default:
                if (state.backtracking>0) {state.failed=true; return retval;}
                NoViableAltException nvae =
                    new NoViableAltException("", 54, 0, input);

                throw nvae;

            }

            switch (alt54) {
                case 1 :
                    // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:336:4: readOperator
                    {
                    root_0 = (EvaluationExpression)adaptor.nil();


                    pushFollow(FOLLOW_readOperator_in_operator2269);
                    readOperator106=readOperator();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, readOperator106.getTree());

                    if ( state.backtracking==0 ) { retval.op = (readOperator106!=null?readOperator106.source:null); }

                    }
                    break;
                case 2 :
                    // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:337:5: writeOperator
                    {
                    root_0 = (EvaluationExpression)adaptor.nil();


                    pushFollow(FOLLOW_writeOperator_in_operator2277);
                    writeOperator107=writeOperator();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, writeOperator107.getTree());

                    if ( state.backtracking==0 ) { retval.op = (writeOperator107!=null?writeOperator107.sink:null); }

                    }
                    break;
                case 3 :
                    // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:338:5: genericOperator
                    {
                    root_0 = (EvaluationExpression)adaptor.nil();


                    pushFollow(FOLLOW_genericOperator_in_operator2285);
                    genericOperator108=genericOperator();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, genericOperator108.getTree());

                    if ( state.backtracking==0 ) { retval.op = (genericOperator108!=null?genericOperator108.op:null); }

                    }
                    break;

            }
            retval.stop = input.LT(-1);


            if ( state.backtracking==0 ) {

            retval.tree = (EvaluationExpression)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }

        catch (RecognitionException e) {
          throw e;
        }

        finally {
        	// do for sure before leaving
            operator_stack.pop();
        }
        return retval;
    }
    // $ANTLR end "operator"


    public static class adhocSource_return extends ParserRuleReturnScope {
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };


    // $ANTLR start "adhocSource"
    // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:340:1: adhocSource : output= VAR '=' exp= arrayCreation ->;
    public final MeteorParser.adhocSource_return adhocSource() throws RecognitionException {
        MeteorParser.adhocSource_return retval = new MeteorParser.adhocSource_return();
        retval.start = input.LT(1);


        EvaluationExpression root_0 = null;

        Token output=null;
        Token char_literal109=null;
        MeteorParser.arrayCreation_return exp =null;


        EvaluationExpression output_tree=null;
        EvaluationExpression char_literal109_tree=null;
        RewriteRuleTokenStream stream_VAR=new RewriteRuleTokenStream(adaptor,"token VAR");
        RewriteRuleTokenStream stream_53=new RewriteRuleTokenStream(adaptor,"token 53");
        RewriteRuleSubtreeStream stream_arrayCreation=new RewriteRuleSubtreeStream(adaptor,"rule arrayCreation");
        try {
            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:340:12: (output= VAR '=' exp= arrayCreation ->)
            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:341:3: output= VAR '=' exp= arrayCreation
            {
            output=(Token)match(input,VAR,FOLLOW_VAR_in_adhocSource2299); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_VAR.add(output);


            char_literal109=(Token)match(input,53,FOLLOW_53_in_adhocSource2301); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_53.add(char_literal109);


            pushFollow(FOLLOW_arrayCreation_in_adhocSource2305);
            exp=arrayCreation();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_arrayCreation.add(exp.getTree());

            if ( state.backtracking==0 ) { 
              Source source = new Source((exp!=null?((EvaluationExpression)exp.tree):null));
              putVariable(output, new JsonStreamExpression(source));
            }

            // AST REWRITE
            // elements: 
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            if ( state.backtracking==0 ) {

            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (EvaluationExpression)adaptor.nil();
            // 345:3: ->
            {
                root_0 = null;
            }


            retval.tree = root_0;
            }

            }

            retval.stop = input.LT(-1);


            if ( state.backtracking==0 ) {

            retval.tree = (EvaluationExpression)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }

        catch (RecognitionException e) {
          throw e;
        }

        finally {
        	// do for sure before leaving
        }
        return retval;
    }
    // $ANTLR end "adhocSource"


    public static class readOperator_return extends ParserRuleReturnScope {
        public Source source;
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };


    // $ANTLR start "readOperator"
    // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:348:1: readOperator returns [Source source] : (output= VAR '=' )? 'read' ( (packageName= ID ':' )? format= ID )? {...}? ID pathExp= ternaryExpression ( confOption[getOperatorInfo($source), $source] )* ->;
    public final MeteorParser.readOperator_return readOperator() throws RecognitionException {
        MeteorParser.readOperator_return retval = new MeteorParser.readOperator_return();
        retval.start = input.LT(1);


        EvaluationExpression root_0 = null;

        Token output=null;
        Token packageName=null;
        Token format=null;
        Token char_literal110=null;
        Token string_literal111=null;
        Token char_literal112=null;
        Token ID113=null;
        MeteorParser.ternaryExpression_return pathExp =null;

        MeteorParser.confOption_return confOption114 =null;


        EvaluationExpression output_tree=null;
        EvaluationExpression packageName_tree=null;
        EvaluationExpression format_tree=null;
        EvaluationExpression char_literal110_tree=null;
        EvaluationExpression string_literal111_tree=null;
        EvaluationExpression char_literal112_tree=null;
        EvaluationExpression ID113_tree=null;
        RewriteRuleTokenStream stream_49=new RewriteRuleTokenStream(adaptor,"token 49");
        RewriteRuleTokenStream stream_VAR=new RewriteRuleTokenStream(adaptor,"token VAR");
        RewriteRuleTokenStream stream_ID=new RewriteRuleTokenStream(adaptor,"token ID");
        RewriteRuleTokenStream stream_53=new RewriteRuleTokenStream(adaptor,"token 53");
        RewriteRuleTokenStream stream_63=new RewriteRuleTokenStream(adaptor,"token 63");
        RewriteRuleSubtreeStream stream_confOption=new RewriteRuleSubtreeStream(adaptor,"rule confOption");
        RewriteRuleSubtreeStream stream_ternaryExpression=new RewriteRuleSubtreeStream(adaptor,"rule ternaryExpression");
         
          ConfObjectInfo<? extends SopremoFormat> formatInfo = null;
          SopremoFormat fileFormat = null;
          String path = null;

        try {
            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:354:2: ( (output= VAR '=' )? 'read' ( (packageName= ID ':' )? format= ID )? {...}? ID pathExp= ternaryExpression ( confOption[getOperatorInfo($source), $source] )* ->)
            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:354:4: (output= VAR '=' )? 'read' ( (packageName= ID ':' )? format= ID )? {...}? ID pathExp= ternaryExpression ( confOption[getOperatorInfo($source), $source] )*
            {
            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:354:4: (output= VAR '=' )?
            int alt55=2;
            int LA55_0 = input.LA(1);

            if ( (LA55_0==VAR) ) {
                alt55=1;
            }
            switch (alt55) {
                case 1 :
                    // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:354:5: output= VAR '='
                    {
                    output=(Token)match(input,VAR,FOLLOW_VAR_in_readOperator2333); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_VAR.add(output);


                    char_literal110=(Token)match(input,53,FOLLOW_53_in_readOperator2335); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_53.add(char_literal110);


                    }
                    break;

            }


            string_literal111=(Token)match(input,63,FOLLOW_63_in_readOperator2343); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_63.add(string_literal111);


            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:355:11: ( (packageName= ID ':' )? format= ID )?
            int alt57=2;
            int LA57_0 = input.LA(1);

            if ( (LA57_0==ID) ) {
                int LA57_1 = input.LA(2);

                if ( (!(((input.LT(1).getText().equals("from"))))) ) {
                    alt57=1;
                }
            }
            switch (alt57) {
                case 1 :
                    // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:355:12: (packageName= ID ':' )? format= ID
                    {
                    // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:355:12: (packageName= ID ':' )?
                    int alt56=2;
                    int LA56_0 = input.LA(1);

                    if ( (LA56_0==ID) ) {
                        int LA56_1 = input.LA(2);

                        if ( (LA56_1==49) ) {
                            alt56=1;
                        }
                    }
                    switch (alt56) {
                        case 1 :
                            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:355:13: packageName= ID ':'
                            {
                            packageName=(Token)match(input,ID,FOLLOW_ID_in_readOperator2349); if (state.failed) return retval; 
                            if ( state.backtracking==0 ) stream_ID.add(packageName);


                            char_literal112=(Token)match(input,49,FOLLOW_49_in_readOperator2351); if (state.failed) return retval; 
                            if ( state.backtracking==0 ) stream_49.add(char_literal112);


                            }
                            break;

                    }


                    format=(Token)match(input,ID,FOLLOW_ID_in_readOperator2356); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_ID.add(format);


                    }
                    break;

            }


            if ( !((input.LT(1).getText().equals("from"))) ) {
                if (state.backtracking>0) {state.failed=true; return retval;}
                throw new FailedPredicateException(input, "readOperator", "input.LT(1).getText().equals(\"from\")");
            }

            ID113=(Token)match(input,ID,FOLLOW_ID_in_readOperator2365); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_ID.add(ID113);


            pushFollow(FOLLOW_ternaryExpression_in_readOperator2369);
            pathExp=ternaryExpression();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_ternaryExpression.add(pathExp.getTree());

            if ( state.backtracking==0 ) { 
              path = makeFilePath((pathExp!=null?((EvaluationExpression)pathExp.tree):null));
              formatInfo = findFormat((packageName!=null?packageName.getText():null), format, path);
              fileFormat = formatInfo.newInstance(); 
              retval.source = new Source(fileFormat, path); 
              if(output != null)
                putVariable(output, new JsonStreamExpression(retval.source));
            }

            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:364:4: ( confOption[getOperatorInfo($source), $source] )*
            loop58:
            do {
                int alt58=2;
                int LA58_0 = input.LA(1);

                if ( (LA58_0==ID) ) {
                    alt58=1;
                }


                switch (alt58) {
            	case 1 :
            	    // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:364:4: confOption[getOperatorInfo($source), $source]
            	    {
            	    pushFollow(FOLLOW_confOption_in_readOperator2374);
            	    confOption114=confOption(getOperatorInfo(retval.source), retval.source);

            	    state._fsp--;
            	    if (state.failed) return retval;
            	    if ( state.backtracking==0 ) stream_confOption.add(confOption114.getTree());

            	    }
            	    break;

            	default :
            	    break loop58;
                }
            } while (true);


            // AST REWRITE
            // elements: 
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            if ( state.backtracking==0 ) {

            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (EvaluationExpression)adaptor.nil();
            // 365:2: ->
            {
                root_0 = null;
            }


            retval.tree = root_0;
            }

            }

            retval.stop = input.LT(-1);


            if ( state.backtracking==0 ) {

            retval.tree = (EvaluationExpression)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }

        catch (RecognitionException e) {
          throw e;
        }

        finally {
        	// do for sure before leaving
        }
        return retval;
    }
    // $ANTLR end "readOperator"


    public static class writeOperator_return extends ParserRuleReturnScope {
        public Sink sink;
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };


    // $ANTLR start "writeOperator"
    // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:368:1: writeOperator returns [Sink sink] : 'write' ( (packageName= ID ':' )? format= ID )? from= VAR {...}? ID pathExp= ternaryExpression ( confOption[getOperatorInfo($sink), $sink] )* ->;
    public final MeteorParser.writeOperator_return writeOperator() throws RecognitionException {
        MeteorParser.writeOperator_return retval = new MeteorParser.writeOperator_return();
        retval.start = input.LT(1);


        EvaluationExpression root_0 = null;

        Token packageName=null;
        Token format=null;
        Token from=null;
        Token string_literal115=null;
        Token char_literal116=null;
        Token ID117=null;
        MeteorParser.ternaryExpression_return pathExp =null;

        MeteorParser.confOption_return confOption118 =null;


        EvaluationExpression packageName_tree=null;
        EvaluationExpression format_tree=null;
        EvaluationExpression from_tree=null;
        EvaluationExpression string_literal115_tree=null;
        EvaluationExpression char_literal116_tree=null;
        EvaluationExpression ID117_tree=null;
        RewriteRuleTokenStream stream_49=new RewriteRuleTokenStream(adaptor,"token 49");
        RewriteRuleTokenStream stream_66=new RewriteRuleTokenStream(adaptor,"token 66");
        RewriteRuleTokenStream stream_VAR=new RewriteRuleTokenStream(adaptor,"token VAR");
        RewriteRuleTokenStream stream_ID=new RewriteRuleTokenStream(adaptor,"token ID");
        RewriteRuleSubtreeStream stream_confOption=new RewriteRuleSubtreeStream(adaptor,"rule confOption");
        RewriteRuleSubtreeStream stream_ternaryExpression=new RewriteRuleSubtreeStream(adaptor,"rule ternaryExpression");
         
          ConfObjectInfo<? extends SopremoFormat> formatInfo = null;
          SopremoFormat fileFormat = null;
          String path = null;

        try {
            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:373:3: ( 'write' ( (packageName= ID ':' )? format= ID )? from= VAR {...}? ID pathExp= ternaryExpression ( confOption[getOperatorInfo($sink), $sink] )* ->)
            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:373:5: 'write' ( (packageName= ID ':' )? format= ID )? from= VAR {...}? ID pathExp= ternaryExpression ( confOption[getOperatorInfo($sink), $sink] )*
            {
            string_literal115=(Token)match(input,66,FOLLOW_66_in_writeOperator2398); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_66.add(string_literal115);


            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:374:5: ( (packageName= ID ':' )? format= ID )?
            int alt60=2;
            int LA60_0 = input.LA(1);

            if ( (LA60_0==ID) ) {
                alt60=1;
            }
            switch (alt60) {
                case 1 :
                    // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:374:6: (packageName= ID ':' )? format= ID
                    {
                    // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:374:6: (packageName= ID ':' )?
                    int alt59=2;
                    int LA59_0 = input.LA(1);

                    if ( (LA59_0==ID) ) {
                        int LA59_1 = input.LA(2);

                        if ( (LA59_1==49) ) {
                            alt59=1;
                        }
                    }
                    switch (alt59) {
                        case 1 :
                            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:374:7: packageName= ID ':'
                            {
                            packageName=(Token)match(input,ID,FOLLOW_ID_in_writeOperator2409); if (state.failed) return retval; 
                            if ( state.backtracking==0 ) stream_ID.add(packageName);


                            char_literal116=(Token)match(input,49,FOLLOW_49_in_writeOperator2411); if (state.failed) return retval; 
                            if ( state.backtracking==0 ) stream_49.add(char_literal116);


                            }
                            break;

                    }


                    format=(Token)match(input,ID,FOLLOW_ID_in_writeOperator2416); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_ID.add(format);


                    }
                    break;

            }


            from=(Token)match(input,VAR,FOLLOW_VAR_in_writeOperator2422); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_VAR.add(from);


            if ( !((input.LT(1).getText().equals("to"))) ) {
                if (state.backtracking>0) {state.failed=true; return retval;}
                throw new FailedPredicateException(input, "writeOperator", "input.LT(1).getText().equals(\"to\")");
            }

            ID117=(Token)match(input,ID,FOLLOW_ID_in_writeOperator2430); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_ID.add(ID117);


            pushFollow(FOLLOW_ternaryExpression_in_writeOperator2434);
            pathExp=ternaryExpression();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_ternaryExpression.add(pathExp.getTree());

            if ( state.backtracking==0 ) { 
              path = makeFilePath((pathExp!=null?((EvaluationExpression)pathExp.tree):null));
              formatInfo = findFormat((packageName!=null?packageName.getText():null), format, path);
              fileFormat = formatInfo.newInstance();
            	retval.sink = new Sink(fileFormat, path);
              retval.sink.setInputs(getVariableSafely(from).getStream());
              this.sinks.add(retval.sink);
            }

            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:383:3: ( confOption[getOperatorInfo($sink), $sink] )*
            loop61:
            do {
                int alt61=2;
                int LA61_0 = input.LA(1);

                if ( (LA61_0==ID) ) {
                    alt61=1;
                }


                switch (alt61) {
            	case 1 :
            	    // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:383:3: confOption[getOperatorInfo($sink), $sink]
            	    {
            	    pushFollow(FOLLOW_confOption_in_writeOperator2438);
            	    confOption118=confOption(getOperatorInfo(retval.sink), retval.sink);

            	    state._fsp--;
            	    if (state.failed) return retval;
            	    if ( state.backtracking==0 ) stream_confOption.add(confOption118.getTree());

            	    }
            	    break;

            	default :
            	    break loop61;
                }
            } while (true);


            // AST REWRITE
            // elements: 
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            if ( state.backtracking==0 ) {

            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (EvaluationExpression)adaptor.nil();
            // 383:46: ->
            {
                root_0 = null;
            }


            retval.tree = root_0;
            }

            }

            retval.stop = input.LT(-1);


            if ( state.backtracking==0 ) {

            retval.tree = (EvaluationExpression)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }

        catch (RecognitionException e) {
          throw e;
        }

        finally {
        	// do for sure before leaving
        }
        return retval;
    }
    // $ANTLR end "writeOperator"


    public static class genericOperator_return extends ParserRuleReturnScope {
        public Operator<?> op;
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };


    // $ANTLR start "genericOperator"
    // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:389:1: genericOperator returns [Operator<?> op] : (targets+= VAR ( ',' targets+= VAR )* '=' )? (packageName= ID ':' )? name= ID {...}? => ( ( VAR )=> input[operatorInfo, $op] ( ( ',' )=> ',' input[operatorInfo, $op] )* )? ( confOption[operatorInfo, $op] )* ->;
    public final MeteorParser.genericOperator_return genericOperator() throws RecognitionException {
        MeteorParser.genericOperator_return retval = new MeteorParser.genericOperator_return();
        retval.start = input.LT(1);


        EvaluationExpression root_0 = null;

        Token packageName=null;
        Token name=null;
        Token char_literal119=null;
        Token char_literal120=null;
        Token char_literal121=null;
        Token char_literal123=null;
        Token targets=null;
        List list_targets=null;
        MeteorParser.input_return input122 =null;

        MeteorParser.input_return input124 =null;

        MeteorParser.confOption_return confOption125 =null;


        EvaluationExpression packageName_tree=null;
        EvaluationExpression name_tree=null;
        EvaluationExpression char_literal119_tree=null;
        EvaluationExpression char_literal120_tree=null;
        EvaluationExpression char_literal121_tree=null;
        EvaluationExpression char_literal123_tree=null;
        EvaluationExpression targets_tree=null;
        RewriteRuleTokenStream stream_49=new RewriteRuleTokenStream(adaptor,"token 49");
        RewriteRuleTokenStream stream_45=new RewriteRuleTokenStream(adaptor,"token 45");
        RewriteRuleTokenStream stream_VAR=new RewriteRuleTokenStream(adaptor,"token VAR");
        RewriteRuleTokenStream stream_ID=new RewriteRuleTokenStream(adaptor,"token ID");
        RewriteRuleTokenStream stream_53=new RewriteRuleTokenStream(adaptor,"token 53");
        RewriteRuleSubtreeStream stream_input=new RewriteRuleSubtreeStream(adaptor,"rule input");
        RewriteRuleSubtreeStream stream_confOption=new RewriteRuleSubtreeStream(adaptor,"rule confOption");
         
          ConfObjectInfo<? extends Operator<?>> operatorInfo;

        try {
            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:395:3: ( (targets+= VAR ( ',' targets+= VAR )* '=' )? (packageName= ID ':' )? name= ID {...}? => ( ( VAR )=> input[operatorInfo, $op] ( ( ',' )=> ',' input[operatorInfo, $op] )* )? ( confOption[operatorInfo, $op] )* ->)
            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:396:2: (targets+= VAR ( ',' targets+= VAR )* '=' )? (packageName= ID ':' )? name= ID {...}? => ( ( VAR )=> input[operatorInfo, $op] ( ( ',' )=> ',' input[operatorInfo, $op] )* )? ( confOption[operatorInfo, $op] )*
            {
            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:396:2: (targets+= VAR ( ',' targets+= VAR )* '=' )?
            int alt63=2;
            int LA63_0 = input.LA(1);

            if ( (LA63_0==VAR) ) {
                alt63=1;
            }
            switch (alt63) {
                case 1 :
                    // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:396:2: targets+= VAR ( ',' targets+= VAR )* '='
                    {
                    targets=(Token)match(input,VAR,FOLLOW_VAR_in_genericOperator2472); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_VAR.add(targets);

                    if (list_targets==null) list_targets=new ArrayList();
                    list_targets.add(targets);


                    // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:396:15: ( ',' targets+= VAR )*
                    loop62:
                    do {
                        int alt62=2;
                        int LA62_0 = input.LA(1);

                        if ( (LA62_0==45) ) {
                            alt62=1;
                        }


                        switch (alt62) {
                    	case 1 :
                    	    // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:396:16: ',' targets+= VAR
                    	    {
                    	    char_literal119=(Token)match(input,45,FOLLOW_45_in_genericOperator2475); if (state.failed) return retval; 
                    	    if ( state.backtracking==0 ) stream_45.add(char_literal119);


                    	    targets=(Token)match(input,VAR,FOLLOW_VAR_in_genericOperator2479); if (state.failed) return retval; 
                    	    if ( state.backtracking==0 ) stream_VAR.add(targets);

                    	    if (list_targets==null) list_targets=new ArrayList();
                    	    list_targets.add(targets);


                    	    }
                    	    break;

                    	default :
                    	    break loop62;
                        }
                    } while (true);


                    char_literal120=(Token)match(input,53,FOLLOW_53_in_genericOperator2483); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_53.add(char_literal120);


                    }
                    break;

            }


            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:397:2: (packageName= ID ':' )?
            int alt64=2;
            int LA64_0 = input.LA(1);

            if ( (LA64_0==ID) ) {
                int LA64_1 = input.LA(2);

                if ( (LA64_1==49) ) {
                    alt64=1;
                }
            }
            switch (alt64) {
                case 1 :
                    // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:397:2: packageName= ID ':'
                    {
                    packageName=(Token)match(input,ID,FOLLOW_ID_in_genericOperator2491); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_ID.add(packageName);


                    char_literal121=(Token)match(input,49,FOLLOW_49_in_genericOperator2493); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_49.add(char_literal121);


                    }
                    break;

            }


            name=(Token)match(input,ID,FOLLOW_ID_in_genericOperator2499); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_ID.add(name);


            if ( !(( (operatorInfo = findOperatorGreedily((packageName!=null?packageName.getText():null), name)) != null  )) ) {
                if (state.backtracking>0) {state.failed=true; return retval;}
                throw new FailedPredicateException(input, "genericOperator", " (operatorInfo = findOperatorGreedily($packageName.text, $name)) != null  ");
            }

            if ( state.backtracking==0 ) { 
              ((operator_scope)operator_stack.peek()).result = retval.op = operatorInfo.newInstance(); 
              // add scope for input variables and recursive definition
              if(state.backtracking == 0) 
                addScope();   
            }

            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:405:2: ( ( VAR )=> input[operatorInfo, $op] ( ( ',' )=> ',' input[operatorInfo, $op] )* )?
            int alt66=2;
            int LA66_0 = input.LA(1);

            if ( (LA66_0==VAR) && (synpred27_Meteor())) {
                alt66=1;
            }
            switch (alt66) {
                case 1 :
                    // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:405:2: ( VAR )=> input[operatorInfo, $op] ( ( ',' )=> ',' input[operatorInfo, $op] )*
                    {
                    pushFollow(FOLLOW_input_in_genericOperator2515);
                    input122=input(operatorInfo, retval.op);

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_input.add(input122.getTree());

                    // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:405:35: ( ( ',' )=> ',' input[operatorInfo, $op] )*
                    loop65:
                    do {
                        int alt65=2;
                        int LA65_0 = input.LA(1);

                        if ( (LA65_0==45) ) {
                            int LA65_2 = input.LA(2);

                            if ( (LA65_2==VAR) ) {
                                int LA65_3 = input.LA(3);

                                if ( (synpred28_Meteor()) ) {
                                    alt65=1;
                                }


                            }


                        }


                        switch (alt65) {
                    	case 1 :
                    	    // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:405:36: ( ',' )=> ',' input[operatorInfo, $op]
                    	    {
                    	    char_literal123=(Token)match(input,45,FOLLOW_45_in_genericOperator2524); if (state.failed) return retval; 
                    	    if ( state.backtracking==0 ) stream_45.add(char_literal123);


                    	    pushFollow(FOLLOW_input_in_genericOperator2526);
                    	    input124=input(operatorInfo, retval.op);

                    	    state._fsp--;
                    	    if (state.failed) return retval;
                    	    if ( state.backtracking==0 ) stream_input.add(input124.getTree());

                    	    }
                    	    break;

                    	default :
                    	    break loop65;
                        }
                    } while (true);


                    }
                    break;

            }


            if ( state.backtracking==0 ) { // register output names for explicit references to output 
              if(list_targets != null)
                for(int index = 0; index < list_targets.size(); index++)
                  putVariable((Token) list_targets.get(index), new JsonStreamExpression(retval.op.getOutput(index)), 1);   
            }

            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:411:11: ( confOption[operatorInfo, $op] )*
            loop67:
            do {
                int alt67=2;
                int LA67_0 = input.LA(1);

                if ( (LA67_0==ID) ) {
                    alt67=1;
                }


                switch (alt67) {
            	case 1 :
            	    // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:411:11: confOption[operatorInfo, $op]
            	    {
            	    pushFollow(FOLLOW_confOption_in_genericOperator2536);
            	    confOption125=confOption(operatorInfo, retval.op);

            	    state._fsp--;
            	    if (state.failed) return retval;
            	    if ( state.backtracking==0 ) stream_confOption.add(confOption125.getTree());

            	    }
            	    break;

            	default :
            	    break loop67;
                }
            } while (true);


            // AST REWRITE
            // elements: 
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            if ( state.backtracking==0 ) {

            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (EvaluationExpression)adaptor.nil();
            // 412:3: ->
            {
                root_0 = null;
            }


            retval.tree = root_0;
            }

            }

            retval.stop = input.LT(-1);


            if ( state.backtracking==0 ) {

            retval.tree = (EvaluationExpression)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
            if ( state.backtracking==0 ) {
              removeScope();
            }
        }

        catch (RecognitionException e) {
          throw e;
        }

        finally {
        	// do for sure before leaving
        }
        return retval;
    }
    // $ANTLR end "genericOperator"


    public static class confOption_return extends ParserRuleReturnScope {
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };


    // $ANTLR start "confOption"
    // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:414:1: confOption[ConfObjectInfo<?> info, ConfigurableSopremoType object] : name= ID {...}? =>expr= ternaryExpression ->;
    public final MeteorParser.confOption_return confOption(ConfObjectInfo<?> info, ConfigurableSopremoType object) throws RecognitionException {
        MeteorParser.confOption_return retval = new MeteorParser.confOption_return();
        retval.start = input.LT(1);


        EvaluationExpression root_0 = null;

        Token name=null;
        MeteorParser.ternaryExpression_return expr =null;


        EvaluationExpression name_tree=null;
        RewriteRuleTokenStream stream_ID=new RewriteRuleTokenStream(adaptor,"token ID");
        RewriteRuleSubtreeStream stream_ternaryExpression=new RewriteRuleSubtreeStream(adaptor,"rule ternaryExpression");

         ConfObjectInfo.ConfObjectPropertyInfo property = null;

        try {
            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:417:3: (name= ID {...}? =>expr= ternaryExpression ->)
            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:418:3: name= ID {...}? =>expr= ternaryExpression
            {
            name=(Token)match(input,ID,FOLLOW_ID_in_confOption2563); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_ID.add(name);


            if ( !(( (property = findPropertyGreedily(object, info, name)) != null )) ) {
                if (state.backtracking>0) {state.failed=true; return retval;}
                throw new FailedPredicateException(input, "confOption", " (property = findPropertyGreedily(object, info, name)) != null ");
            }

            pushFollow(FOLLOW_ternaryExpression_in_confOption2573);
            expr=ternaryExpression();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_ternaryExpression.add(expr.getTree());

            if ( state.backtracking==0 ) { property.setValue(object, (expr!=null?((EvaluationExpression)expr.tree):null)); }

            // AST REWRITE
            // elements: 
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            if ( state.backtracking==0 ) {

            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (EvaluationExpression)adaptor.nil();
            // 420:69: ->
            {
                root_0 = null;
            }


            retval.tree = root_0;
            }

            }

            retval.stop = input.LT(-1);


            if ( state.backtracking==0 ) {

            retval.tree = (EvaluationExpression)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }

        catch (RecognitionException e) {
          throw e;
        }

        finally {
        	// do for sure before leaving
        }
        return retval;
    }
    // $ANTLR end "confOption"


    public static class input_return extends ParserRuleReturnScope {
        EvaluationExpression tree;
        public Object getTree() { return tree; }
    };


    // $ANTLR start "input"
    // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:422:1: input[ConfObjectInfo<?> info, Operator<?> object] : (name= VAR IN )? from= VAR ({...}? =>expr= ternaryExpression )? ->;
    public final MeteorParser.input_return input(ConfObjectInfo<?> info, Operator<?> object) throws RecognitionException {
        MeteorParser.input_return retval = new MeteorParser.input_return();
        retval.start = input.LT(1);


        EvaluationExpression root_0 = null;

        Token name=null;
        Token from=null;
        Token IN126=null;
        MeteorParser.ternaryExpression_return expr =null;


        EvaluationExpression name_tree=null;
        EvaluationExpression from_tree=null;
        EvaluationExpression IN126_tree=null;
        RewriteRuleTokenStream stream_VAR=new RewriteRuleTokenStream(adaptor,"token VAR");
        RewriteRuleTokenStream stream_IN=new RewriteRuleTokenStream(adaptor,"token IN");
        RewriteRuleSubtreeStream stream_ternaryExpression=new RewriteRuleSubtreeStream(adaptor,"rule ternaryExpression");

         ConfObjectInfo.ConfObjectIndexedPropertyInfo inputProperty = null;

        try {
            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:425:3: ( (name= VAR IN )? from= VAR ({...}? =>expr= ternaryExpression )? ->)
            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:425:5: (name= VAR IN )? from= VAR ({...}? =>expr= ternaryExpression )?
            {
            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:425:5: (name= VAR IN )?
            int alt68=2;
            int LA68_0 = input.LA(1);

            if ( (LA68_0==VAR) ) {
                int LA68_1 = input.LA(2);

                if ( (LA68_1==IN) ) {
                    alt68=1;
                }
            }
            switch (alt68) {
                case 1 :
                    // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:425:6: name= VAR IN
                    {
                    name=(Token)match(input,VAR,FOLLOW_VAR_in_input2595); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_VAR.add(name);


                    IN126=(Token)match(input,IN,FOLLOW_IN_in_input2597); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_IN.add(IN126);


                    }
                    break;

            }


            from=(Token)match(input,VAR,FOLLOW_VAR_in_input2603); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_VAR.add(from);


            if ( state.backtracking==0 ) { 
              int inputIndex = ((operator_scope)operator_stack.peek()).numInputs++;
              JsonStreamExpression input = getVariableSafely(from);
              object.setInput(inputIndex, input.getStream());
              
              if(operator_stack.size() == 1) {
            	  JsonStreamExpression inputExpression = new JsonStreamExpression(input.getStream(), inputIndex);
            	  putVariable(name != null ? name : from, inputExpression);
              }
            }

            // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:436:2: ({...}? =>expr= ternaryExpression )?
            int alt69=2;
            int LA69_0 = input.LA(1);

            if ( (LA69_0==DECIMAL||LA69_0==FN||LA69_0==INTEGER||(LA69_0 >= STRING && LA69_0 <= UINT)||LA69_0==VAR||LA69_0==36||(LA69_0 >= 40 && LA69_0 <= 41)||LA69_0==44||LA69_0==47||LA69_0==59||(LA69_0 >= 61 && LA69_0 <= 62)||LA69_0==64||LA69_0==67||LA69_0==70) && (( (findInputPropertyRelunctantly(object, info, input.LT(1), false) != null) ))) {
                alt69=1;
            }
            else if ( (LA69_0==ID) ) {
                int LA69_5 = input.LA(2);

                if ( (( (findInputPropertyRelunctantly(object, info, input.LT(1), false) != null) )) ) {
                    alt69=1;
                }
            }
            switch (alt69) {
                case 1 :
                    // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:436:2: {...}? =>expr= ternaryExpression
                    {
                    if ( !(( (findInputPropertyRelunctantly(object, info, input.LT(1), false) != null) )) ) {
                        if (state.backtracking>0) {state.failed=true; return retval;}
                        throw new FailedPredicateException(input, "input", " (findInputPropertyRelunctantly(object, info, input.LT(1), false) != null) ");
                    }

                    if ( state.backtracking==0 ) { inputProperty = findInputPropertyRelunctantly(object, info, input.LT(1), true); }

                    pushFollow(FOLLOW_ternaryExpression_in_input2620);
                    expr=ternaryExpression();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) stream_ternaryExpression.add(expr.getTree());

                    if ( state.backtracking==0 ) { inputProperty.setValue(object, ((operator_scope)operator_stack.peek()).numInputs-1, (expr!=null?((EvaluationExpression)expr.tree):null)); }

                    }
                    break;

            }


            // AST REWRITE
            // elements: 
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            if ( state.backtracking==0 ) {

            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (EvaluationExpression)adaptor.nil();
            // 439:4: ->
            {
                root_0 = null;
            }


            retval.tree = root_0;
            }

            }

            retval.stop = input.LT(-1);


            if ( state.backtracking==0 ) {

            retval.tree = (EvaluationExpression)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }

        catch (RecognitionException e) {
          throw e;
        }

        finally {
        	// do for sure before leaving
        }
        return retval;
    }
    // $ANTLR end "input"

    // $ANTLR start synpred1_Meteor
    public final void synpred1_Meteor_fragment() throws RecognitionException {
        // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:110:5: ( ID '=' FN )
        // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:110:6: ID '=' FN
        {
        match(input,ID,FOLLOW_ID_in_synpred1_Meteor222); if (state.failed) return ;

        match(input,53,FOLLOW_53_in_synpred1_Meteor224); if (state.failed) return ;

        match(input,FN,FOLLOW_FN_in_synpred1_Meteor226); if (state.failed) return ;

        }

    }
    // $ANTLR end synpred1_Meteor

    // $ANTLR start synpred2_Meteor
    public final void synpred2_Meteor_fragment() throws RecognitionException {
        // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:111:5: ( ID '=' JAVAUDF )
        // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:111:6: ID '=' JAVAUDF
        {
        match(input,ID,FOLLOW_ID_in_synpred2_Meteor238); if (state.failed) return ;

        match(input,53,FOLLOW_53_in_synpred2_Meteor240); if (state.failed) return ;

        match(input,JAVAUDF,FOLLOW_JAVAUDF_in_synpred2_Meteor242); if (state.failed) return ;

        }

    }
    // $ANTLR end synpred2_Meteor

    // $ANTLR start synpred3_Meteor
    public final void synpred3_Meteor_fragment() throws RecognitionException {
        // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:147:5: ( ID ( ID | VAR ) )
        // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:147:6: ID ( ID | VAR )
        {
        match(input,ID,FOLLOW_ID_in_synpred3_Meteor441); if (state.failed) return ;

        if ( input.LA(1)==ID||input.LA(1)==VAR ) {
            input.consume();
            state.errorRecovery=false;
            state.failed=false;
        }
        else {
            if (state.backtracking>0) {state.failed=true; return ;}
            MismatchedSetException mse = new MismatchedSetException(null,input);
            throw mse;
        }


        }

    }
    // $ANTLR end synpred3_Meteor

    // $ANTLR start synpred4_Meteor
    public final void synpred4_Meteor_fragment() throws RecognitionException {
        // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:153:4: ( orExpression '?' )
        // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:153:5: orExpression '?'
        {
        pushFollow(FOLLOW_orExpression_in_synpred4_Meteor478);
        orExpression();

        state._fsp--;
        if (state.failed) return ;

        match(input,57,FOLLOW_57_in_synpred4_Meteor480); if (state.failed) return ;

        }

    }
    // $ANTLR end synpred4_Meteor

    // $ANTLR start synpred5_Meteor
    public final void synpred5_Meteor_fragment() throws RecognitionException {
        // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:154:9: ( '(' )
        // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:154:10: '('
        {
        match(input,41,FOLLOW_41_in_synpred5_Meteor496); if (state.failed) return ;

        }

    }
    // $ANTLR end synpred5_Meteor

    // $ANTLR start synpred6_Meteor
    public final void synpred6_Meteor_fragment() throws RecognitionException {
        // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:157:4: ( orExpression IF )
        // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:157:5: orExpression IF
        {
        pushFollow(FOLLOW_orExpression_in_synpred6_Meteor552);
        orExpression();

        state._fsp--;
        if (state.failed) return ;

        match(input,IF,FOLLOW_IF_in_synpred6_Meteor554); if (state.failed) return ;

        }

    }
    // $ANTLR end synpred6_Meteor

    // $ANTLR start synpred7_Meteor
    public final void synpred7_Meteor_fragment() throws RecognitionException {
        // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:209:4: ( '(' ID ')' )
        // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:209:5: '(' ID ')'
        {
        match(input,41,FOLLOW_41_in_synpred7_Meteor1023); if (state.failed) return ;

        match(input,ID,FOLLOW_ID_in_synpred7_Meteor1025); if (state.failed) return ;

        match(input,42,FOLLOW_42_in_synpred7_Meteor1027); if (state.failed) return ;

        }

    }
    // $ANTLR end synpred7_Meteor

    // $ANTLR start synpred8_Meteor
    public final void synpred8_Meteor_fragment() throws RecognitionException {
        // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:216:5: ( pathExpression[EvaluationExpression.VALUE] )
        // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:216:6: pathExpression[EvaluationExpression.VALUE]
        {
        pushFollow(FOLLOW_pathExpression_in_synpred8_Meteor1090);
        pathExpression(EvaluationExpression.VALUE);

        state._fsp--;
        if (state.failed) return ;

        }

    }
    // $ANTLR end synpred8_Meteor

    // $ANTLR start synpred9_Meteor
    public final void synpred9_Meteor_fragment() throws RecognitionException {
        // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:224:5: ( '?.' ID '(' )
        // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:224:6: '?.' ID '('
        {
        match(input,58,FOLLOW_58_in_synpred9_Meteor1146); if (state.failed) return ;

        match(input,ID,FOLLOW_ID_in_synpred9_Meteor1148); if (state.failed) return ;

        match(input,41,FOLLOW_41_in_synpred9_Meteor1150); if (state.failed) return ;

        }

    }
    // $ANTLR end synpred9_Meteor

    // $ANTLR start synpred10_Meteor
    public final void synpred10_Meteor_fragment() throws RecognitionException {
        // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:225:8: ( pathSegment )
        // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:225:9: pathSegment
        {
        pushFollow(FOLLOW_pathSegment_in_synpred10_Meteor1169);
        pathSegment();

        state._fsp--;
        if (state.failed) return ;

        }

    }
    // $ANTLR end synpred10_Meteor

    // $ANTLR start synpred11_Meteor
    public final void synpred11_Meteor_fragment() throws RecognitionException {
        // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:228:5: ( '.' ID '(' )
        // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:228:6: '.' ID '('
        {
        match(input,48,FOLLOW_48_in_synpred11_Meteor1217); if (state.failed) return ;

        match(input,ID,FOLLOW_ID_in_synpred11_Meteor1219); if (state.failed) return ;

        match(input,41,FOLLOW_41_in_synpred11_Meteor1221); if (state.failed) return ;

        }

    }
    // $ANTLR end synpred11_Meteor

    // $ANTLR start synpred12_Meteor
    public final void synpred12_Meteor_fragment() throws RecognitionException {
        // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:229:8: ( pathSegment )
        // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:229:9: pathSegment
        {
        pushFollow(FOLLOW_pathSegment_in_synpred12_Meteor1240);
        pathSegment();

        state._fsp--;
        if (state.failed) return ;

        }

    }
    // $ANTLR end synpred12_Meteor

    // $ANTLR start synpred13_Meteor
    public final void synpred13_Meteor_fragment() throws RecognitionException {
        // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:232:6: ( pathSegment )
        // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:232:7: pathSegment
        {
        pushFollow(FOLLOW_pathSegment_in_synpred13_Meteor1282);
        pathSegment();

        state._fsp--;
        if (state.failed) return ;

        }

    }
    // $ANTLR end synpred13_Meteor

    // $ANTLR start synpred14_Meteor
    public final void synpred14_Meteor_fragment() throws RecognitionException {
        // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:239:5: ( '?.' )
        // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:239:6: '?.'
        {
        match(input,58,FOLLOW_58_in_synpred14_Meteor1336); if (state.failed) return ;

        }

    }
    // $ANTLR end synpred14_Meteor

    // $ANTLR start synpred15_Meteor
    public final void synpred15_Meteor_fragment() throws RecognitionException {
        // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:240:5: ( '.' )
        // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:240:6: '.'
        {
        match(input,48,FOLLOW_48_in_synpred15_Meteor1367); if (state.failed) return ;

        }

    }
    // $ANTLR end synpred15_Meteor

    // $ANTLR start synpred16_Meteor
    public final void synpred16_Meteor_fragment() throws RecognitionException {
        // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:241:5: ( '[' )
        // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:241:6: '['
        {
        match(input,59,FOLLOW_59_in_synpred16_Meteor1396); if (state.failed) return ;

        }

    }
    // $ANTLR end synpred16_Meteor

    // $ANTLR start synpred17_Meteor
    public final void synpred17_Meteor_fragment() throws RecognitionException {
        // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:244:19: ( '.' methodCall[null] )
        // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:244:20: '.' methodCall[null]
        {
        match(input,48,FOLLOW_48_in_synpred17_Meteor1419); if (state.failed) return ;

        pushFollow(FOLLOW_methodCall_in_synpred17_Meteor1421);
        methodCall(null);

        state._fsp--;
        if (state.failed) return ;

        }

    }
    // $ANTLR end synpred17_Meteor

    // $ANTLR start synpred18_Meteor
    public final void synpred18_Meteor_fragment() throws RecognitionException {
        // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:254:4: ( ID '(' )
        // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:254:5: ID '('
        {
        match(input,ID,FOLLOW_ID_in_synpred18_Meteor1560); if (state.failed) return ;

        match(input,41,FOLLOW_41_in_synpred18_Meteor1562); if (state.failed) return ;

        }

    }
    // $ANTLR end synpred18_Meteor

    // $ANTLR start synpred19_Meteor
    public final void synpred19_Meteor_fragment() throws RecognitionException {
        // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:256:5: ( FN )
        // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:256:6: FN
        {
        match(input,FN,FOLLOW_FN_in_synpred19_Meteor1578); if (state.failed) return ;

        }

    }
    // $ANTLR end synpred19_Meteor

    // $ANTLR start synpred21_Meteor
    public final void synpred21_Meteor_fragment() throws RecognitionException {
        // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:288:10: ( ID ':' )
        // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:288:11: ID ':'
        {
        match(input,ID,FOLLOW_ID_in_synpred21_Meteor1824); if (state.failed) return ;

        match(input,49,FOLLOW_49_in_synpred21_Meteor1826); if (state.failed) return ;

        }

    }
    // $ANTLR end synpred21_Meteor

    // $ANTLR start synpred22_Meteor
    public final void synpred22_Meteor_fragment() throws RecognitionException {
        // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:292:5: ( ID ':' )
        // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:292:6: ID ':'
        {
        match(input,ID,FOLLOW_ID_in_synpred22_Meteor1870); if (state.failed) return ;

        match(input,49,FOLLOW_49_in_synpred22_Meteor1872); if (state.failed) return ;

        }

    }
    // $ANTLR end synpred22_Meteor

    // $ANTLR start synpred23_Meteor
    public final void synpred23_Meteor_fragment() throws RecognitionException {
        // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:294:6: ( '\"' ID '\"' ':' )
        // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:294:7: '\"' ID '\"' ':'
        {
        match(input,38,FOLLOW_38_in_synpred23_Meteor1899); if (state.failed) return ;

        match(input,ID,FOLLOW_ID_in_synpred23_Meteor1901); if (state.failed) return ;

        match(input,38,FOLLOW_38_in_synpred23_Meteor1903); if (state.failed) return ;

        match(input,49,FOLLOW_49_in_synpred23_Meteor1905); if (state.failed) return ;

        }

    }
    // $ANTLR end synpred23_Meteor

    // $ANTLR start synpred24_Meteor
    public final void synpred24_Meteor_fragment() throws RecognitionException {
        // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:296:5: ( VAR '.' STAR )
        // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:296:6: VAR '.' STAR
        {
        match(input,VAR,FOLLOW_VAR_in_synpred24_Meteor1935); if (state.failed) return ;

        match(input,48,FOLLOW_48_in_synpred24_Meteor1937); if (state.failed) return ;

        match(input,STAR,FOLLOW_STAR_in_synpred24_Meteor1939); if (state.failed) return ;

        }

    }
    // $ANTLR end synpred24_Meteor

    // $ANTLR start synpred25_Meteor
    public final void synpred25_Meteor_fragment() throws RecognitionException {
        // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:297:5: ( VAR )
        // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:297:6: VAR
        {
        match(input,VAR,FOLLOW_VAR_in_synpred25_Meteor1958); if (state.failed) return ;

        }

    }
    // $ANTLR end synpred25_Meteor

    // $ANTLR start synpred26_Meteor
    public final void synpred26_Meteor_fragment() throws RecognitionException {
        // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:298:5: ( ':' )
        // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:298:6: ':'
        {
        match(input,49,FOLLOW_49_in_synpred26_Meteor1973); if (state.failed) return ;

        }

    }
    // $ANTLR end synpred26_Meteor

    // $ANTLR start synpred27_Meteor
    public final void synpred27_Meteor_fragment() throws RecognitionException {
        // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:405:2: ( VAR )
        // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:405:3: VAR
        {
        match(input,VAR,FOLLOW_VAR_in_synpred27_Meteor2511); if (state.failed) return ;

        }

    }
    // $ANTLR end synpred27_Meteor

    // $ANTLR start synpred28_Meteor
    public final void synpred28_Meteor_fragment() throws RecognitionException {
        // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:405:36: ( ',' )
        // /home/arvid/workspace/sopremo/meteor/meteor-meteor/src/main/java/eu/stratosphere/meteor/Meteor.g:405:37: ','
        {
        match(input,45,FOLLOW_45_in_synpred28_Meteor2520); if (state.failed) return ;

        }

    }
    // $ANTLR end synpred28_Meteor

    // Delegated rules

    public final boolean synpred22_Meteor() {
        state.backtracking++;
        int start = input.mark();
        try {
            synpred22_Meteor_fragment(); // can never throw exception
        } catch (RecognitionException re) {
            System.err.println("impossible: "+re);
        }
        boolean success = !state.failed;
        input.rewind(start);
        state.backtracking--;
        state.failed=false;
        return success;
    }
    public final boolean synpred28_Meteor() {
        state.backtracking++;
        int start = input.mark();
        try {
            synpred28_Meteor_fragment(); // can never throw exception
        } catch (RecognitionException re) {
            System.err.println("impossible: "+re);
        }
        boolean success = !state.failed;
        input.rewind(start);
        state.backtracking--;
        state.failed=false;
        return success;
    }
    public final boolean synpred13_Meteor() {
        state.backtracking++;
        int start = input.mark();
        try {
            synpred13_Meteor_fragment(); // can never throw exception
        } catch (RecognitionException re) {
            System.err.println("impossible: "+re);
        }
        boolean success = !state.failed;
        input.rewind(start);
        state.backtracking--;
        state.failed=false;
        return success;
    }
    public final boolean synpred8_Meteor() {
        state.backtracking++;
        int start = input.mark();
        try {
            synpred8_Meteor_fragment(); // can never throw exception
        } catch (RecognitionException re) {
            System.err.println("impossible: "+re);
        }
        boolean success = !state.failed;
        input.rewind(start);
        state.backtracking--;
        state.failed=false;
        return success;
    }
    public final boolean synpred18_Meteor() {
        state.backtracking++;
        int start = input.mark();
        try {
            synpred18_Meteor_fragment(); // can never throw exception
        } catch (RecognitionException re) {
            System.err.println("impossible: "+re);
        }
        boolean success = !state.failed;
        input.rewind(start);
        state.backtracking--;
        state.failed=false;
        return success;
    }
    public final boolean synpred1_Meteor() {
        state.backtracking++;
        int start = input.mark();
        try {
            synpred1_Meteor_fragment(); // can never throw exception
        } catch (RecognitionException re) {
            System.err.println("impossible: "+re);
        }
        boolean success = !state.failed;
        input.rewind(start);
        state.backtracking--;
        state.failed=false;
        return success;
    }
    public final boolean synpred12_Meteor() {
        state.backtracking++;
        int start = input.mark();
        try {
            synpred12_Meteor_fragment(); // can never throw exception
        } catch (RecognitionException re) {
            System.err.println("impossible: "+re);
        }
        boolean success = !state.failed;
        input.rewind(start);
        state.backtracking--;
        state.failed=false;
        return success;
    }
    public final boolean synpred10_Meteor() {
        state.backtracking++;
        int start = input.mark();
        try {
            synpred10_Meteor_fragment(); // can never throw exception
        } catch (RecognitionException re) {
            System.err.println("impossible: "+re);
        }
        boolean success = !state.failed;
        input.rewind(start);
        state.backtracking--;
        state.failed=false;
        return success;
    }
    public final boolean synpred11_Meteor() {
        state.backtracking++;
        int start = input.mark();
        try {
            synpred11_Meteor_fragment(); // can never throw exception
        } catch (RecognitionException re) {
            System.err.println("impossible: "+re);
        }
        boolean success = !state.failed;
        input.rewind(start);
        state.backtracking--;
        state.failed=false;
        return success;
    }
    public final boolean synpred23_Meteor() {
        state.backtracking++;
        int start = input.mark();
        try {
            synpred23_Meteor_fragment(); // can never throw exception
        } catch (RecognitionException re) {
            System.err.println("impossible: "+re);
        }
        boolean success = !state.failed;
        input.rewind(start);
        state.backtracking--;
        state.failed=false;
        return success;
    }
    public final boolean synpred7_Meteor() {
        state.backtracking++;
        int start = input.mark();
        try {
            synpred7_Meteor_fragment(); // can never throw exception
        } catch (RecognitionException re) {
            System.err.println("impossible: "+re);
        }
        boolean success = !state.failed;
        input.rewind(start);
        state.backtracking--;
        state.failed=false;
        return success;
    }
    public final boolean synpred9_Meteor() {
        state.backtracking++;
        int start = input.mark();
        try {
            synpred9_Meteor_fragment(); // can never throw exception
        } catch (RecognitionException re) {
            System.err.println("impossible: "+re);
        }
        boolean success = !state.failed;
        input.rewind(start);
        state.backtracking--;
        state.failed=false;
        return success;
    }
    public final boolean synpred25_Meteor() {
        state.backtracking++;
        int start = input.mark();
        try {
            synpred25_Meteor_fragment(); // can never throw exception
        } catch (RecognitionException re) {
            System.err.println("impossible: "+re);
        }
        boolean success = !state.failed;
        input.rewind(start);
        state.backtracking--;
        state.failed=false;
        return success;
    }
    public final boolean synpred14_Meteor() {
        state.backtracking++;
        int start = input.mark();
        try {
            synpred14_Meteor_fragment(); // can never throw exception
        } catch (RecognitionException re) {
            System.err.println("impossible: "+re);
        }
        boolean success = !state.failed;
        input.rewind(start);
        state.backtracking--;
        state.failed=false;
        return success;
    }
    public final boolean synpred26_Meteor() {
        state.backtracking++;
        int start = input.mark();
        try {
            synpred26_Meteor_fragment(); // can never throw exception
        } catch (RecognitionException re) {
            System.err.println("impossible: "+re);
        }
        boolean success = !state.failed;
        input.rewind(start);
        state.backtracking--;
        state.failed=false;
        return success;
    }
    public final boolean synpred19_Meteor() {
        state.backtracking++;
        int start = input.mark();
        try {
            synpred19_Meteor_fragment(); // can never throw exception
        } catch (RecognitionException re) {
            System.err.println("impossible: "+re);
        }
        boolean success = !state.failed;
        input.rewind(start);
        state.backtracking--;
        state.failed=false;
        return success;
    }
    public final boolean synpred17_Meteor() {
        state.backtracking++;
        int start = input.mark();
        try {
            synpred17_Meteor_fragment(); // can never throw exception
        } catch (RecognitionException re) {
            System.err.println("impossible: "+re);
        }
        boolean success = !state.failed;
        input.rewind(start);
        state.backtracking--;
        state.failed=false;
        return success;
    }
    public final boolean synpred2_Meteor() {
        state.backtracking++;
        int start = input.mark();
        try {
            synpred2_Meteor_fragment(); // can never throw exception
        } catch (RecognitionException re) {
            System.err.println("impossible: "+re);
        }
        boolean success = !state.failed;
        input.rewind(start);
        state.backtracking--;
        state.failed=false;
        return success;
    }
    public final boolean synpred6_Meteor() {
        state.backtracking++;
        int start = input.mark();
        try {
            synpred6_Meteor_fragment(); // can never throw exception
        } catch (RecognitionException re) {
            System.err.println("impossible: "+re);
        }
        boolean success = !state.failed;
        input.rewind(start);
        state.backtracking--;
        state.failed=false;
        return success;
    }
    public final boolean synpred15_Meteor() {
        state.backtracking++;
        int start = input.mark();
        try {
            synpred15_Meteor_fragment(); // can never throw exception
        } catch (RecognitionException re) {
            System.err.println("impossible: "+re);
        }
        boolean success = !state.failed;
        input.rewind(start);
        state.backtracking--;
        state.failed=false;
        return success;
    }
    public final boolean synpred3_Meteor() {
        state.backtracking++;
        int start = input.mark();
        try {
            synpred3_Meteor_fragment(); // can never throw exception
        } catch (RecognitionException re) {
            System.err.println("impossible: "+re);
        }
        boolean success = !state.failed;
        input.rewind(start);
        state.backtracking--;
        state.failed=false;
        return success;
    }
    public final boolean synpred5_Meteor() {
        state.backtracking++;
        int start = input.mark();
        try {
            synpred5_Meteor_fragment(); // can never throw exception
        } catch (RecognitionException re) {
            System.err.println("impossible: "+re);
        }
        boolean success = !state.failed;
        input.rewind(start);
        state.backtracking--;
        state.failed=false;
        return success;
    }
    public final boolean synpred27_Meteor() {
        state.backtracking++;
        int start = input.mark();
        try {
            synpred27_Meteor_fragment(); // can never throw exception
        } catch (RecognitionException re) {
            System.err.println("impossible: "+re);
        }
        boolean success = !state.failed;
        input.rewind(start);
        state.backtracking--;
        state.failed=false;
        return success;
    }
    public final boolean synpred4_Meteor() {
        state.backtracking++;
        int start = input.mark();
        try {
            synpred4_Meteor_fragment(); // can never throw exception
        } catch (RecognitionException re) {
            System.err.println("impossible: "+re);
        }
        boolean success = !state.failed;
        input.rewind(start);
        state.backtracking--;
        state.failed=false;
        return success;
    }
    public final boolean synpred21_Meteor() {
        state.backtracking++;
        int start = input.mark();
        try {
            synpred21_Meteor_fragment(); // can never throw exception
        } catch (RecognitionException re) {
            System.err.println("impossible: "+re);
        }
        boolean success = !state.failed;
        input.rewind(start);
        state.backtracking--;
        state.failed=false;
        return success;
    }
    public final boolean synpred24_Meteor() {
        state.backtracking++;
        int start = input.mark();
        try {
            synpred24_Meteor_fragment(); // can never throw exception
        } catch (RecognitionException re) {
            System.err.println("impossible: "+re);
        }
        boolean success = !state.failed;
        input.rewind(start);
        state.backtracking--;
        state.failed=false;
        return success;
    }
    public final boolean synpred16_Meteor() {
        state.backtracking++;
        int start = input.mark();
        try {
            synpred16_Meteor_fragment(); // can never throw exception
        } catch (RecognitionException re) {
            System.err.println("impossible: "+re);
        }
        boolean success = !state.failed;
        input.rewind(start);
        state.backtracking--;
        state.failed=false;
        return success;
    }


 

    public static final BitSet FOLLOW_statement_in_script131 = new BitSet(new long[]{0x0004000000000000L});
    public static final BitSet FOLLOW_50_in_script133 = new BitSet(new long[]{0x8004000400008002L,0x0000000000000006L});
    public static final BitSet FOLLOW_operator_in_statement147 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_packageImport_in_statement151 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_adhocSource_in_statement155 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_definition_in_statement159 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_functionCall_in_statement169 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_65_in_packageImport186 = new BitSet(new long[]{0x0000000000008000L});
    public static final BitSet FOLLOW_ID_in_packageImport190 = new BitSet(new long[]{0x0000200000000002L});
    public static final BitSet FOLLOW_45_in_packageImport201 = new BitSet(new long[]{0x0000000000008000L});
    public static final BitSet FOLLOW_ID_in_packageImport205 = new BitSet(new long[]{0x0000200000000002L});
    public static final BitSet FOLLOW_functionDefinition_in_definition230 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_javaudf_in_definition246 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_constantDefinition_in_definition252 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_ID_in_functionDefinition264 = new BitSet(new long[]{0x0020000000000000L});
    public static final BitSet FOLLOW_53_in_functionDefinition266 = new BitSet(new long[]{0x0000000000002000L});
    public static final BitSet FOLLOW_inlineFunction_in_functionDefinition270 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_ID_in_constantDefinition289 = new BitSet(new long[]{0x0020000000000000L});
    public static final BitSet FOLLOW_53_in_constantDefinition291 = new BitSet(new long[]{0x68009314C008A080L,0x0000000000000049L});
    public static final BitSet FOLLOW_ternaryExpression_in_constantDefinition295 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_FN_in_inlineFunction321 = new BitSet(new long[]{0x0000020000000000L});
    public static final BitSet FOLLOW_41_in_inlineFunction323 = new BitSet(new long[]{0x0000040000008000L});
    public static final BitSet FOLLOW_ID_in_inlineFunction332 = new BitSet(new long[]{0x0000240000000000L});
    public static final BitSet FOLLOW_45_in_inlineFunction339 = new BitSet(new long[]{0x0000000000008000L});
    public static final BitSet FOLLOW_ID_in_inlineFunction343 = new BitSet(new long[]{0x0000240000000000L});
    public static final BitSet FOLLOW_42_in_inlineFunction354 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000008L});
    public static final BitSet FOLLOW_67_in_inlineFunction364 = new BitSet(new long[]{0xE8009314C008A080L,0x000000000000004DL});
    public static final BitSet FOLLOW_expression_in_inlineFunction368 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000020L});
    public static final BitSet FOLLOW_69_in_inlineFunction370 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_ID_in_javaudf390 = new BitSet(new long[]{0x0020000000000000L});
    public static final BitSet FOLLOW_53_in_javaudf392 = new BitSet(new long[]{0x0000000000100000L});
    public static final BitSet FOLLOW_JAVAUDF_in_javaudf394 = new BitSet(new long[]{0x0000020000000000L});
    public static final BitSet FOLLOW_41_in_javaudf396 = new BitSet(new long[]{0x0000000040000000L});
    public static final BitSet FOLLOW_STRING_in_javaudf400 = new BitSet(new long[]{0x0000040000000000L});
    public static final BitSet FOLLOW_42_in_javaudf402 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_ternaryExpression_in_contextAwareExpression430 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_operatorExpression_in_expression453 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_ternaryExpression_in_expression459 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_orExpression_in_ternaryExpression486 = new BitSet(new long[]{0x0200000000000000L});
    public static final BitSet FOLLOW_57_in_ternaryExpression492 = new BitSet(new long[]{0x68029314C008A080L,0x0000000000000049L});
    public static final BitSet FOLLOW_41_in_ternaryExpression501 = new BitSet(new long[]{0x68009314C008A080L,0x0000000000000049L});
    public static final BitSet FOLLOW_orExpression_in_ternaryExpression505 = new BitSet(new long[]{0x0000040000000000L});
    public static final BitSet FOLLOW_42_in_ternaryExpression507 = new BitSet(new long[]{0x0002000000000000L});
    public static final BitSet FOLLOW_orExpression_in_ternaryExpression516 = new BitSet(new long[]{0x0002000000000000L});
    public static final BitSet FOLLOW_49_in_ternaryExpression527 = new BitSet(new long[]{0x68009314C008A080L,0x0000000000000049L});
    public static final BitSet FOLLOW_orExpression_in_ternaryExpression531 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_orExpression_in_ternaryExpression560 = new BitSet(new long[]{0x0000000000010000L});
    public static final BitSet FOLLOW_IF_in_ternaryExpression562 = new BitSet(new long[]{0x68009314C008A080L,0x0000000000000049L});
    public static final BitSet FOLLOW_orExpression_in_ternaryExpression566 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_orExpression_in_ternaryExpression589 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_andExpression_in_orExpression602 = new BitSet(new long[]{0x0000000002000002L,0x0000000000000010L});
    public static final BitSet FOLLOW_OR_in_orExpression606 = new BitSet(new long[]{0x68009314C008A080L,0x0000000000000049L});
    public static final BitSet FOLLOW_68_in_orExpression610 = new BitSet(new long[]{0x68009314C008A080L,0x0000000000000049L});
    public static final BitSet FOLLOW_andExpression_in_orExpression615 = new BitSet(new long[]{0x0000000002000002L,0x0000000000000010L});
    public static final BitSet FOLLOW_elementExpression_in_andExpression644 = new BitSet(new long[]{0x0000008000000012L});
    public static final BitSet FOLLOW_AND_in_andExpression648 = new BitSet(new long[]{0x68009314C008A080L,0x0000000000000049L});
    public static final BitSet FOLLOW_39_in_andExpression652 = new BitSet(new long[]{0x68009314C008A080L,0x0000000000000049L});
    public static final BitSet FOLLOW_elementExpression_in_andExpression657 = new BitSet(new long[]{0x0000008000000012L});
    public static final BitSet FOLLOW_comparisonExpression_in_elementExpression686 = new BitSet(new long[]{0x0000000000420002L});
    public static final BitSet FOLLOW_NOT_in_elementExpression691 = new BitSet(new long[]{0x0000000000020000L});
    public static final BitSet FOLLOW_IN_in_elementExpression694 = new BitSet(new long[]{0x68009314C008A080L,0x0000000000000049L});
    public static final BitSet FOLLOW_elementExpression_in_elementExpression698 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_arithmeticExpression_in_comparisonExpression739 = new BitSet(new long[]{0x01D8002000000002L});
    public static final BitSet FOLLOW_52_in_comparisonExpression745 = new BitSet(new long[]{0x68009314C008A080L,0x0000000000000049L});
    public static final BitSet FOLLOW_56_in_comparisonExpression751 = new BitSet(new long[]{0x68009314C008A080L,0x0000000000000049L});
    public static final BitSet FOLLOW_51_in_comparisonExpression757 = new BitSet(new long[]{0x68009314C008A080L,0x0000000000000049L});
    public static final BitSet FOLLOW_55_in_comparisonExpression763 = new BitSet(new long[]{0x68009314C008A080L,0x0000000000000049L});
    public static final BitSet FOLLOW_54_in_comparisonExpression769 = new BitSet(new long[]{0x68009314C008A080L,0x0000000000000049L});
    public static final BitSet FOLLOW_37_in_comparisonExpression775 = new BitSet(new long[]{0x68009314C008A080L,0x0000000000000049L});
    public static final BitSet FOLLOW_comparisonExpression_in_comparisonExpression780 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_multiplicationExpression_in_arithmeticExpression860 = new BitSet(new long[]{0x0000480000000002L});
    public static final BitSet FOLLOW_43_in_arithmeticExpression866 = new BitSet(new long[]{0x68009314C008A080L,0x0000000000000049L});
    public static final BitSet FOLLOW_46_in_arithmeticExpression872 = new BitSet(new long[]{0x68009314C008A080L,0x0000000000000049L});
    public static final BitSet FOLLOW_arithmeticExpression_in_arithmeticExpression877 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_preincrementExpression_in_multiplicationExpression920 = new BitSet(new long[]{0x0000000030000002L});
    public static final BitSet FOLLOW_STAR_in_multiplicationExpression926 = new BitSet(new long[]{0x68009314C008A080L,0x0000000000000049L});
    public static final BitSet FOLLOW_SLASH_in_multiplicationExpression932 = new BitSet(new long[]{0x68009314C008A080L,0x0000000000000049L});
    public static final BitSet FOLLOW_preincrementExpression_in_multiplicationExpression937 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_44_in_preincrementExpression978 = new BitSet(new long[]{0x68009314C008A080L,0x0000000000000049L});
    public static final BitSet FOLLOW_preincrementExpression_in_preincrementExpression980 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_47_in_preincrementExpression985 = new BitSet(new long[]{0x68009314C008A080L,0x0000000000000049L});
    public static final BitSet FOLLOW_preincrementExpression_in_preincrementExpression987 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_unaryExpression_in_preincrementExpression992 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_castExpression_in_unaryExpression1011 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_41_in_castExpression1031 = new BitSet(new long[]{0x0000000000008000L});
    public static final BitSet FOLLOW_ID_in_castExpression1035 = new BitSet(new long[]{0x0000040000000000L});
    public static final BitSet FOLLOW_42_in_castExpression1037 = new BitSet(new long[]{0x68000304C008A080L,0x0000000000000009L});
    public static final BitSet FOLLOW_generalPathExpression_in_castExpression1041 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_generalPathExpression_in_castExpression1054 = new BitSet(new long[]{0x0000000000008002L});
    public static final BitSet FOLLOW_ID_in_castExpression1059 = new BitSet(new long[]{0x0000000000008000L});
    public static final BitSet FOLLOW_ID_in_castExpression1063 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_valueExpression_in_generalPathExpression1082 = new BitSet(new long[]{0x0C01000000000002L});
    public static final BitSet FOLLOW_pathExpression_in_generalPathExpression1097 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_pathExpression_in_contextAwarePathExpression1126 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_58_in_pathExpression1154 = new BitSet(new long[]{0x0000000000008000L});
    public static final BitSet FOLLOW_methodCall_in_pathExpression1158 = new BitSet(new long[]{0x0C01000000000002L});
    public static final BitSet FOLLOW_pathExpression_in_pathExpression1175 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_48_in_pathExpression1225 = new BitSet(new long[]{0x0000000000008000L});
    public static final BitSet FOLLOW_methodCall_in_pathExpression1229 = new BitSet(new long[]{0x0C01000000000002L});
    public static final BitSet FOLLOW_pathExpression_in_pathExpression1246 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_pathSegment_in_pathExpression1272 = new BitSet(new long[]{0x0C01000000000002L});
    public static final BitSet FOLLOW_pathExpression_in_pathExpression1288 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_58_in_pathSegment1340 = new BitSet(new long[]{0x0000000000008000L});
    public static final BitSet FOLLOW_ID_in_pathSegment1344 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_48_in_pathSegment1372 = new BitSet(new long[]{0x0000000000008000L});
    public static final BitSet FOLLOW_ID_in_pathSegment1376 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_arrayAccess_in_pathSegment1401 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_59_in_arrayAccess1411 = new BitSet(new long[]{0x0000000020000000L});
    public static final BitSet FOLLOW_STAR_in_arrayAccess1413 = new BitSet(new long[]{0x1000000000000000L});
    public static final BitSet FOLLOW_60_in_arrayAccess1415 = new BitSet(new long[]{0x0C01000000000000L});
    public static final BitSet FOLLOW_48_in_arrayAccess1426 = new BitSet(new long[]{0x0000000000008000L});
    public static final BitSet FOLLOW_methodCall_in_arrayAccess1430 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_pathSegment_in_arrayAccess1453 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_59_in_arrayAccess1474 = new BitSet(new long[]{0x0000000080080000L});
    public static final BitSet FOLLOW_INTEGER_in_arrayAccess1479 = new BitSet(new long[]{0x1000000000000000L});
    public static final BitSet FOLLOW_UINT_in_arrayAccess1485 = new BitSet(new long[]{0x1000000000000000L});
    public static final BitSet FOLLOW_60_in_arrayAccess1488 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_59_in_arrayAccess1506 = new BitSet(new long[]{0x0000000080080000L});
    public static final BitSet FOLLOW_INTEGER_in_arrayAccess1511 = new BitSet(new long[]{0x0002000000000000L});
    public static final BitSet FOLLOW_UINT_in_arrayAccess1517 = new BitSet(new long[]{0x0002000000000000L});
    public static final BitSet FOLLOW_49_in_arrayAccess1520 = new BitSet(new long[]{0x0000000080080000L});
    public static final BitSet FOLLOW_INTEGER_in_arrayAccess1525 = new BitSet(new long[]{0x1000000000000000L});
    public static final BitSet FOLLOW_UINT_in_arrayAccess1531 = new BitSet(new long[]{0x1000000000000000L});
    public static final BitSet FOLLOW_60_in_arrayAccess1534 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_functionCall_in_valueExpression1566 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_functionReference_in_valueExpression1571 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_inlineFunction_in_valueExpression1584 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_parenthesesExpression_in_valueExpression1598 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_literal_in_valueExpression1604 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_VAR_in_valueExpression1610 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_constantExpression_in_valueExpression1620 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_arrayCreation_in_valueExpression1627 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_objectCreation_in_valueExpression1633 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_ID_in_constantExpression1655 = new BitSet(new long[]{0x0002000000000000L});
    public static final BitSet FOLLOW_49_in_constantExpression1657 = new BitSet(new long[]{0x0000000000008000L});
    public static final BitSet FOLLOW_ID_in_constantExpression1663 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_operator_in_operatorExpression1696 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_41_in_parenthesesExpression1715 = new BitSet(new long[]{0xE8009314C008A080L,0x000000000000004DL});
    public static final BitSet FOLLOW_expression_in_parenthesesExpression1717 = new BitSet(new long[]{0x0000040000000000L});
    public static final BitSet FOLLOW_42_in_parenthesesExpression1719 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_ID_in_methodCall1749 = new BitSet(new long[]{0x0002000000000000L});
    public static final BitSet FOLLOW_49_in_methodCall1751 = new BitSet(new long[]{0x0000000000008000L});
    public static final BitSet FOLLOW_ID_in_methodCall1757 = new BitSet(new long[]{0x0000020000000000L});
    public static final BitSet FOLLOW_41_in_methodCall1759 = new BitSet(new long[]{0xE8009714C008A080L,0x000000000000004DL});
    public static final BitSet FOLLOW_expression_in_methodCall1768 = new BitSet(new long[]{0x0000240000000000L});
    public static final BitSet FOLLOW_45_in_methodCall1777 = new BitSet(new long[]{0xE8009314C008A080L,0x000000000000004DL});
    public static final BitSet FOLLOW_expression_in_methodCall1782 = new BitSet(new long[]{0x0000240000000000L});
    public static final BitSet FOLLOW_42_in_methodCall1794 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_methodCall_in_functionCall1809 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_40_in_functionReference1820 = new BitSet(new long[]{0x0000000000008000L});
    public static final BitSet FOLLOW_ID_in_functionReference1832 = new BitSet(new long[]{0x0002000000000000L});
    public static final BitSet FOLLOW_49_in_functionReference1834 = new BitSet(new long[]{0x0000000000008000L});
    public static final BitSet FOLLOW_ID_in_functionReference1840 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_ID_in_fieldAssignment1876 = new BitSet(new long[]{0x0002000000000000L});
    public static final BitSet FOLLOW_49_in_fieldAssignment1878 = new BitSet(new long[]{0xE8009314C008A080L,0x000000000000004DL});
    public static final BitSet FOLLOW_expression_in_fieldAssignment1880 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_38_in_fieldAssignment1909 = new BitSet(new long[]{0x0000000000008000L});
    public static final BitSet FOLLOW_ID_in_fieldAssignment1911 = new BitSet(new long[]{0x0000004000000000L});
    public static final BitSet FOLLOW_38_in_fieldAssignment1913 = new BitSet(new long[]{0x0002000000000000L});
    public static final BitSet FOLLOW_49_in_fieldAssignment1915 = new BitSet(new long[]{0xE8009314C008A080L,0x000000000000004DL});
    public static final BitSet FOLLOW_expression_in_fieldAssignment1917 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_VAR_in_fieldAssignment1943 = new BitSet(new long[]{0x0001000000000000L});
    public static final BitSet FOLLOW_48_in_fieldAssignment1945 = new BitSet(new long[]{0x0000000020000000L});
    public static final BitSet FOLLOW_STAR_in_fieldAssignment1947 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_generalPathExpression_in_fieldAssignment1964 = new BitSet(new long[]{0x0002000000000002L});
    public static final BitSet FOLLOW_49_in_fieldAssignment1977 = new BitSet(new long[]{0xE8009314C008A080L,0x000000000000004DL});
    public static final BitSet FOLLOW_expression_in_fieldAssignment1981 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_valueExpression_in_fieldAssignment2011 = new BitSet(new long[]{0x0002000000000000L});
    public static final BitSet FOLLOW_49_in_fieldAssignment2013 = new BitSet(new long[]{0xE8009314C008A080L,0x000000000000004DL});
    public static final BitSet FOLLOW_expression_in_fieldAssignment2017 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_67_in_objectCreation2055 = new BitSet(new long[]{0x68000344C008A080L,0x0000000000000029L});
    public static final BitSet FOLLOW_fieldAssignment_in_objectCreation2058 = new BitSet(new long[]{0x0000200000000000L,0x0000000000000020L});
    public static final BitSet FOLLOW_45_in_objectCreation2061 = new BitSet(new long[]{0x68000344C008A080L,0x0000000000000009L});
    public static final BitSet FOLLOW_fieldAssignment_in_objectCreation2063 = new BitSet(new long[]{0x0000200000000000L,0x0000000000000020L});
    public static final BitSet FOLLOW_45_in_objectCreation2067 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000020L});
    public static final BitSet FOLLOW_69_in_objectCreation2072 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_64_in_literal2110 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_61_in_literal2126 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_DECIMAL_in_literal2142 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_STRING_in_literal2158 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_UINT_in_literal2176 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_INTEGER_in_literal2182 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_62_in_literal2198 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_59_in_arrayCreation2222 = new BitSet(new long[]{0xF8009314C008A080L,0x000000000000004DL});
    public static final BitSet FOLLOW_expression_in_arrayCreation2227 = new BitSet(new long[]{0x1000200000000000L});
    public static final BitSet FOLLOW_45_in_arrayCreation2230 = new BitSet(new long[]{0xE8009314C008A080L,0x000000000000004DL});
    public static final BitSet FOLLOW_expression_in_arrayCreation2234 = new BitSet(new long[]{0x1000200000000000L});
    public static final BitSet FOLLOW_45_in_arrayCreation2238 = new BitSet(new long[]{0x1000000000000000L});
    public static final BitSet FOLLOW_60_in_arrayCreation2243 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_readOperator_in_operator2269 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_writeOperator_in_operator2277 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_genericOperator_in_operator2285 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_VAR_in_adhocSource2299 = new BitSet(new long[]{0x0020000000000000L});
    public static final BitSet FOLLOW_53_in_adhocSource2301 = new BitSet(new long[]{0x0800000000000000L});
    public static final BitSet FOLLOW_arrayCreation_in_adhocSource2305 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_VAR_in_readOperator2333 = new BitSet(new long[]{0x0020000000000000L});
    public static final BitSet FOLLOW_53_in_readOperator2335 = new BitSet(new long[]{0x8000000000000000L});
    public static final BitSet FOLLOW_63_in_readOperator2343 = new BitSet(new long[]{0x0000000000008000L});
    public static final BitSet FOLLOW_ID_in_readOperator2349 = new BitSet(new long[]{0x0002000000000000L});
    public static final BitSet FOLLOW_49_in_readOperator2351 = new BitSet(new long[]{0x0000000000008000L});
    public static final BitSet FOLLOW_ID_in_readOperator2356 = new BitSet(new long[]{0x0000000000008000L});
    public static final BitSet FOLLOW_ID_in_readOperator2365 = new BitSet(new long[]{0x68009314C008A080L,0x0000000000000049L});
    public static final BitSet FOLLOW_ternaryExpression_in_readOperator2369 = new BitSet(new long[]{0x0000000000008002L});
    public static final BitSet FOLLOW_confOption_in_readOperator2374 = new BitSet(new long[]{0x0000000000008002L});
    public static final BitSet FOLLOW_66_in_writeOperator2398 = new BitSet(new long[]{0x0000000400008000L});
    public static final BitSet FOLLOW_ID_in_writeOperator2409 = new BitSet(new long[]{0x0002000000000000L});
    public static final BitSet FOLLOW_49_in_writeOperator2411 = new BitSet(new long[]{0x0000000000008000L});
    public static final BitSet FOLLOW_ID_in_writeOperator2416 = new BitSet(new long[]{0x0000000400000000L});
    public static final BitSet FOLLOW_VAR_in_writeOperator2422 = new BitSet(new long[]{0x0000000000008000L});
    public static final BitSet FOLLOW_ID_in_writeOperator2430 = new BitSet(new long[]{0x68009314C008A080L,0x0000000000000049L});
    public static final BitSet FOLLOW_ternaryExpression_in_writeOperator2434 = new BitSet(new long[]{0x0000000000008002L});
    public static final BitSet FOLLOW_confOption_in_writeOperator2438 = new BitSet(new long[]{0x0000000000008002L});
    public static final BitSet FOLLOW_VAR_in_genericOperator2472 = new BitSet(new long[]{0x0020200000000000L});
    public static final BitSet FOLLOW_45_in_genericOperator2475 = new BitSet(new long[]{0x0000000400000000L});
    public static final BitSet FOLLOW_VAR_in_genericOperator2479 = new BitSet(new long[]{0x0020200000000000L});
    public static final BitSet FOLLOW_53_in_genericOperator2483 = new BitSet(new long[]{0x0000000000008000L});
    public static final BitSet FOLLOW_ID_in_genericOperator2491 = new BitSet(new long[]{0x0002000000000000L});
    public static final BitSet FOLLOW_49_in_genericOperator2493 = new BitSet(new long[]{0x0000000000008000L});
    public static final BitSet FOLLOW_ID_in_genericOperator2499 = new BitSet(new long[]{0x0000000400008002L});
    public static final BitSet FOLLOW_input_in_genericOperator2515 = new BitSet(new long[]{0x0000200000008002L});
    public static final BitSet FOLLOW_45_in_genericOperator2524 = new BitSet(new long[]{0x0000000400000000L});
    public static final BitSet FOLLOW_input_in_genericOperator2526 = new BitSet(new long[]{0x0000200000008002L});
    public static final BitSet FOLLOW_confOption_in_genericOperator2536 = new BitSet(new long[]{0x0000000000008002L});
    public static final BitSet FOLLOW_ID_in_confOption2563 = new BitSet(new long[]{0x68009314C008A080L,0x0000000000000049L});
    public static final BitSet FOLLOW_ternaryExpression_in_confOption2573 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_VAR_in_input2595 = new BitSet(new long[]{0x0000000000020000L});
    public static final BitSet FOLLOW_IN_in_input2597 = new BitSet(new long[]{0x0000000400000000L});
    public static final BitSet FOLLOW_VAR_in_input2603 = new BitSet(new long[]{0x68009314C008A082L,0x0000000000000049L});
    public static final BitSet FOLLOW_ternaryExpression_in_input2620 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_ID_in_synpred1_Meteor222 = new BitSet(new long[]{0x0020000000000000L});
    public static final BitSet FOLLOW_53_in_synpred1_Meteor224 = new BitSet(new long[]{0x0000000000002000L});
    public static final BitSet FOLLOW_FN_in_synpred1_Meteor226 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_ID_in_synpred2_Meteor238 = new BitSet(new long[]{0x0020000000000000L});
    public static final BitSet FOLLOW_53_in_synpred2_Meteor240 = new BitSet(new long[]{0x0000000000100000L});
    public static final BitSet FOLLOW_JAVAUDF_in_synpred2_Meteor242 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_ID_in_synpred3_Meteor441 = new BitSet(new long[]{0x0000000400008000L});
    public static final BitSet FOLLOW_set_in_synpred3_Meteor443 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_orExpression_in_synpred4_Meteor478 = new BitSet(new long[]{0x0200000000000000L});
    public static final BitSet FOLLOW_57_in_synpred4_Meteor480 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_41_in_synpred5_Meteor496 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_orExpression_in_synpred6_Meteor552 = new BitSet(new long[]{0x0000000000010000L});
    public static final BitSet FOLLOW_IF_in_synpred6_Meteor554 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_41_in_synpred7_Meteor1023 = new BitSet(new long[]{0x0000000000008000L});
    public static final BitSet FOLLOW_ID_in_synpred7_Meteor1025 = new BitSet(new long[]{0x0000040000000000L});
    public static final BitSet FOLLOW_42_in_synpred7_Meteor1027 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_pathExpression_in_synpred8_Meteor1090 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_58_in_synpred9_Meteor1146 = new BitSet(new long[]{0x0000000000008000L});
    public static final BitSet FOLLOW_ID_in_synpred9_Meteor1148 = new BitSet(new long[]{0x0000020000000000L});
    public static final BitSet FOLLOW_41_in_synpred9_Meteor1150 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_pathSegment_in_synpred10_Meteor1169 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_48_in_synpred11_Meteor1217 = new BitSet(new long[]{0x0000000000008000L});
    public static final BitSet FOLLOW_ID_in_synpred11_Meteor1219 = new BitSet(new long[]{0x0000020000000000L});
    public static final BitSet FOLLOW_41_in_synpred11_Meteor1221 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_pathSegment_in_synpred12_Meteor1240 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_pathSegment_in_synpred13_Meteor1282 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_58_in_synpred14_Meteor1336 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_48_in_synpred15_Meteor1367 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_59_in_synpred16_Meteor1396 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_48_in_synpred17_Meteor1419 = new BitSet(new long[]{0x0000000000008000L});
    public static final BitSet FOLLOW_methodCall_in_synpred17_Meteor1421 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_ID_in_synpred18_Meteor1560 = new BitSet(new long[]{0x0000020000000000L});
    public static final BitSet FOLLOW_41_in_synpred18_Meteor1562 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_FN_in_synpred19_Meteor1578 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_ID_in_synpred21_Meteor1824 = new BitSet(new long[]{0x0002000000000000L});
    public static final BitSet FOLLOW_49_in_synpred21_Meteor1826 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_ID_in_synpred22_Meteor1870 = new BitSet(new long[]{0x0002000000000000L});
    public static final BitSet FOLLOW_49_in_synpred22_Meteor1872 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_38_in_synpred23_Meteor1899 = new BitSet(new long[]{0x0000000000008000L});
    public static final BitSet FOLLOW_ID_in_synpred23_Meteor1901 = new BitSet(new long[]{0x0000004000000000L});
    public static final BitSet FOLLOW_38_in_synpred23_Meteor1903 = new BitSet(new long[]{0x0002000000000000L});
    public static final BitSet FOLLOW_49_in_synpred23_Meteor1905 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_VAR_in_synpred24_Meteor1935 = new BitSet(new long[]{0x0001000000000000L});
    public static final BitSet FOLLOW_48_in_synpred24_Meteor1937 = new BitSet(new long[]{0x0000000020000000L});
    public static final BitSet FOLLOW_STAR_in_synpred24_Meteor1939 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_VAR_in_synpred25_Meteor1958 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_49_in_synpred26_Meteor1973 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_VAR_in_synpred27_Meteor2511 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_45_in_synpred28_Meteor2520 = new BitSet(new long[]{0x0000000000000002L});

}
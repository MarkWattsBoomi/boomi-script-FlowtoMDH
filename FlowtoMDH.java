

import java.io.IOException;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.math.BigDecimal;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Scanner;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.Transformer;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.Text;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;


import com.boomi.document.scripting.DataContextImpl;
import com.boomi.execution.ExecutionUtil;  
import java.util.logging.Logger;  

import groovy.json.*;


public class RecordQueryRequestMaker
{

	public String make(DataContextImpl dataContext) 
	{

        Logger logger = ExecutionUtil.getBaseLogger();  
        
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm'Z'");
        
        String json = "";
        
        //Prepare a RecordQueryRequest object
        RecordQueryRequest rqr = new RecordQueryRequest();
        
        //get input document
        if(dataContext.getDataCount() > 0)
        {
            InputStream is = dataContext.getStream(0); 
            
            logger.info("Input Stream");
            Scanner s = new Scanner(is).useDelimiter("\\A");
            String req = s.hasNext() ? s.next() : "";
            
            NodeList expressions = null;
            NodeList filters = null;
            NodeList sorts = null;
            
            String joiner = "OR";
            
            
            if(req.length() > 0)
            {
            	DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
    
            	factory.setNamespaceAware(true);
            	DocumentBuilder builder = factory.newDocumentBuilder();
            	logger.info("REQ=" + req);
            	Document doc = builder.parse(new InputSource(new StringReader(req)));
            	
            	
            	
            	//this gets the query parameters from the Flow where
            	expressions = doc.getElementsByTagName("expression");
            	filters = doc.getElementsByTagName("nestedExpression");
            	sorts = doc.getElementsByTagName("sort");
            }
        	
        	
        
        	
        	Node item = null;
        	String field = "";
        	String direction = "";
        	String comparator = "";
        	String value="";
        	
        	
        	//get outer grouping
        	//if(expressions)
        	//{
        	//    joiner = expressions.item(0).getAttributes().getNamedItem("operator").getTextContent().toUpperCase() == "AND"?"AND":"OR";
        	//}
        	
        	
        	//get filters - each one will become a RecordQueryRequestFilter
        	if(filters)
        	{
            	for(int fPos = 0 ; fPos < filters.getLength() ; fPos++)
            	{
            		item = filters.item(fPos);
            		Element e = (Element)item;
            		field=e.getAttribute("property")
            		comparator=e.getAttribute("operator").toUpperCase().trim();
            		
            		//there could be multi values yyyy-MM-dd loop over them adding the joiner
            		NodeList values=((Element)item).getElementsByTagName("argument");
            		
            		logger.info("VAL_LEN=" + values.getLength());
            		
            		for(int vPos = 0 ; vPos < values.getLength() ; vPos++)
            		{
            			String tvalue = values.item(vPos).getTextContent();
            			logger.info("VAL=" + tvalue);
            			
            			
            			if(value.length()>0)
                		{
            				value += " " + joiner + " ";
                		}
                		
            			value += tvalue;
            		}
     
            		rqr.addFilter(item, comparator, value);
            	}
        	}
        	
        	
        	//get sorts
        	if(sorts)
        	{
            	for(int sPos = 0 ; sPos < sorts.getLength() ; sPos++)
            	{
            		item = sorts.item(sPos);
            		Element e = (Element)item;
            		field=e.getAttribute("property");
            		direction=e.getAttribute("sortOrder").toUpperCase();
            		
            		rqr.addSort(field, direction);
            	}
        	}
        }

	
	    //now create JSON Document
        json = rqr.makeDocument();
        	
	    //return JSON Document
    	Properties docProps = dataContext.getProperties(0);
        dataContext.storeStream(new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8)),docProps);
	
	    return json;
	}

	
	
    public class RecordQueryRequest
    {
        private List<String> view = new ArrayList<String>();
        private List<RecordQueryRequestSort> sort = new ArrayList<RecordQueryRequestSort>();
        private List<RecordQueryRequestFilter> filter = new ArrayList<RecordQueryRequestFilter>();
        
        public RecordQueryRequest(){}
        
        public List<String> getView()
        {
            return view;
        }
        
        public List<RecordQueryRequestSort> getSort()
        {
            return sort;
        }
        
        public List<RecordQueryRequestFilter> getFilter()
        {
            return filter;
        }
        
        public void addView(String fieldId)
        {
            view.add(fieldId);
    	}
    	
    	public void addSort(String fieldId, String direction)
        {
            RecordQueryRequestSort rqrs = new RecordQueryRequestSort(fieldId,direction);
    	    sort.add(rqrs);
    	}
    	
        public void addFilter(String fieldId, String operator, String value)
        {
    	    RecordQueryRequestFilter rqrf = new RecordQueryRequestFilter(fieldId, operator, value);
    	    filter.add(rqrf);
    	}
    	
    	public String makeDocument()
    	{
    	    DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
           	DocumentBuilder builder = factory.newDocumentBuilder();
           	Document doc = builder.newDocument();
           	
           	Element nRoot = null;
           	Element nView = null;
           	Element nSort = null;
           	Element nFilter = null;
           	Element n1 = null;
           	Element n2 = null;
           	Element n3 = null;
           	Element n4 = null;
           	Text t = null;
           	
           	//add Root 
           	nRoot = doc.createElement("RecordQueryRequest");
           	nRoot.setAttribute("limit", "100");
           	nRoot.setAttribute("offsetToken", "0");
           	doc.appendChild(nRoot);
           	
           	
           	//add base elements 
           	nView = doc.createElement("view");
           	nRoot.appendChild(nView);
           	
           	if(view && view.length() > 0)
           	{
           	    for(String s : view)
           	    {
               	    n1 = doc.createElement("fieldId");
               	    n1.appendChild(doc.createTextNode(s));
               	    nView.appendChild(n1);
           	    }
           	}
           	
           	//add sort fields 
           	nSort = doc.createElement("sort");
           	nRoot.appendChild(nSort);
           	
           	if(sort && sort.size() > 0)
           	{
           	    /*
           	    for(RecordQueryRequestSort rqrs : sort)
           	    {
               	    n1 = doc.createElement("sortField");
               	    nSort.appendChild(n1);
               	    
               	    n2 = doc.createElement("fieldId");
               	    n2.appendChild(doc.createTextNode(rqrs.getFieldId()));
               	    n1.appendChild(n2);
               	    
               	    n3 = doc.createElement("direction");
               	    n3.appendChild(doc.createTextNode(rqrs.getDirection()));
               	    n1.appendChild(n3);
           	    }
           	    */
           	}
           	
           	/*
           	//add filters 
           	nFilter = doc.createElement("filter");
           	nRoot.appendChild(nFilter);
           	
           	n1 = doc.createElement("createdDate");
           	n1.appendChild(doc.createElement("from"));
           	n1.appendChild(doc.createElement("to"));
           	nFilter.appendChild(n1);
           	
           	n1 = doc.createElement("creatingSourceId");
           	nFilter.appendChild(n1);
           	
           	n1 = doc.createElement("sourceLink");
           	n1.appendChild(doc.createElement("sourceId"));
           	n1.appendChild(doc.createElement("linkType"));
           	nFilter.appendChild(n1);
           	
           	n1 = doc.createElement("tags");
           	//n1.appendChild(doc.createElement("tagName");)
           	nFilter.appendChild(n1);

           	n1 = doc.createElement("unresolvedReference");
           	n1.appendChild(doc.createElement("fieldId"));
           	n1.appendChild(doc.createElement("sourceId"));
           	nFilter.appendChild(n1);
 
           	n1 = doc.createElement("updatedDate");
           	n1.appendChild(doc.createElement("from"));
           	n1.appendChild(doc.createElement("to"));
           	nFilter.appendChild(n1);

           	if(filter && filter.length() > 0)
           	{
           	    for(RecordQueryRequestFilter rqrf : filter)
           	    {
               	    n1 = doc.createElement("fieldValue");
               	    nFilter.appendChild(n1);
               	    
               	    n2 = doc.createElement("fieldId");
               	    n2.appendChild(doc.createTextNode(rqrs.getFieldId()));
               	    n1.appendChild(n2);
               	    
               	    n3 = doc.createElement("operator");
               	    n3.appendChild(doc.createTextNode(rqrs.getOperator()));
               	    n1.appendChild(n3);
               	    
               	    n4 = doc.createElement("value");
               	    n4.appendChild(doc.createTextNode(rqrs.getValue()));
               	    n1.appendChild(n4);
           	    }
           	}
           	*/
           	
           	DOMSource domSource = new DOMSource(doc);
            StringWriter writer = new StringWriter();
            StreamResult result = new StreamResult(writer);
            TransformerFactory tf = TransformerFactory.newInstance();
            Transformer transformer = tf.newTransformer();
            transformer.transform(domSource, result);
            return writer.toString();
    	}
    }
	
    public class RecordQueryRequestSort
    {
        private String fieldId = "";
        private String direction = "";
        
        public RecordQueryRequestSort(){}
    
        public RecordQueryRequestSort(String fieldId, String direction)
        {
            this.setFieldId(fieldId);
            this.setDirection(direction);
        }
        
        public void setFieldId(String fieldId)
        {
           this.fieldId = fieldId; 
        }
        
        public String getFieldId()
        {
           return this.fieldId; 
        }
        
        //guarantee we only have ASC or DESC
        public void setDirection(String direction)
        {
           switch(direction.toUpperCase())
           {
               case "DESC":
               case "DESCENDING":
               case "DOWN":
                   this.direction = "desc";
                   break;
               
               default:
                    this.direction = "asc";
                   break;
           }
        }
        
        public String getDirection()
        {
           return this.direction; 
        }
    }
	
    public class RecordQueryRequestFilter
    {
        private String fieldId = "";
        private String operator = "";
        private String value = "";
        
        public RecordQueryRequestFilter(){}
        
        public RecordQueryRequestFilter(String fieldId, String operator, String value)
        {
            this.setFieldId(fieldId);
            this.setOperator(operator);
            this.setValue(value);
        }
        
        public void setFieldId(String fieldId)
        {
            this.fieldId = fieldId;
        }
        
        public String getFieldId()
        {
            return this.fieldId;
        }
        
        public void setOperator(String operator)
        {
            String result = "";
            switch(operator)
    		{
        		case "EQUAL":
        			result  = "=";
        			break;
        			
        		case "NOT_EQUAL":
        			result = "!=";
        			break;
        			
        		case "LESS_THAN":
        			result = "<";
        			break;
        			
        		case "LESS_THAN_OR_EQUAL":
        			result = "<=";
        			break;
        			
        		case "GREATER_THAN":
        			result = ">";
        			break;
        			
        		case "GREATER_THAN_OR_EQUAL":
        			result = ">=";
        			break;
        		
        		case "STARTS_WITH":
        			result = "LIKE";
        			break;
        			
        		case "ENDS_WITH":
        			result = "LIKE";
        			break;
        			
        		case "CONTAINS":
        			result = "LIKE";
        			break;
        			
        		case "IS_EMPTY":
        			result = "LIKE";
        			break;
    		}
            this.operator = result;
        }
        
        public String getOperator()
        {
            return this.operator;
        }
        
        public void setValue(String value)
        {
            this.value = value;
        }
        
        public String getValue()
        {
            return this.value;
        }
    }
}
	

RecordQueryRequestMaker rqrm = new RecordQueryRequestMaker();
String json = rqrm.make(dataContext);

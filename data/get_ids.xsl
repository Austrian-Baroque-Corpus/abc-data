<?xml version="1.0" encoding="UTF-8"?>
<xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
    xmlns:xs="http://www.w3.org/2001/XMLSchema"
    xmlns:math="http://www.w3.org/2005/xpath-functions/math"
    xmlns:tei="http://www.tei-c.org/ns/1.0"
    xmlns:fcs="http://clarin.eu/fcs/1.0" xmlns:sru="http://www.loc.gov/zing/srw/"
    xmlns:_="urn:local"
    exclude-result-prefixes="xs math tei"
    version="3.0">
    
 
    
    <xsl:function name="_:partPrefix">
        <xsl:param name="id" as="attribute(xml:id)"/>
        <xsl:value-of select="translate(substring-after($id,'_'),'0123456789','')"/>
    </xsl:function>
    
    <xsl:output method="text" indent="yes"/>
    <xsl:param name="pathToDocs">file:/home/danielschopper/data/abc-data/data/editions</xsl:param>
    <xsl:param name="outputMode">concOutput</xsl:param>
    
    <xsl:param name="baseUrlOld">https://acdh.oeaw.ac.at/abacus/get/</xsl:param>
    <xsl:param name="baseUrlNew">https://abacus.acdh-ch-dev.oeaw.ac.at</xsl:param>
    <xsl:param name="debug">false</xsl:param>
    
    <xsl:template match="/">
        <xsl:for-each select="collection($pathToDocs||'?select=*.xml')">
            <xsl:choose>
                <xsl:when test="$outputMode='ruleOutput'">
                    <xsl:call-template name="ruleOutput"/>
                </xsl:when>
                <xsl:otherwise>
                    <xsl:call-template name="concOutput"/>
                </xsl:otherwise>
            </xsl:choose>
        </xsl:for-each>
    </xsl:template>
        
    <xsl:template name="ruleOutput">
        <xsl:variable name="workPrefix" select="substring-before((//tei:pb)[1]/@xml:id,'_')"/>
        <xsl:variable name="workID" select="//tei:idno[@type='cr-xq-mets']"/>
        
        <work prefix="{$workPrefix}">concOutput
            <xsl:for-each-group select="//tei:pb" group-by="_:partPrefix(@xml:id)">
                <xsl:variable name="partPrefix" select="current-grouping-key()"/>
                <xsl:variable name="first" select="current-group()[1]"/>
                <xsl:variable name="firstId" select="current-group()[1]/@xml:id"/>
                <xsl:variable name="firstNo" select="substring-after($firstId,$partPrefix)"/>
                <xsl:variable name="firstPos" select="count($first/preceding::tei:pb)+1"/>
                <xsl:variable name="last" select="current-group()[last()]"/>
                <xsl:variable name="lastId" select="$last/@xml:id"/>
                <xsl:variable name="lastNo" select="substring-after($lastId,$partPrefix)"/>
                <xsl:variable name="lastPos" select="count($last/preceding::tei:pb)+1"/>
                <prefix n="{current-grouping-key()}">
                    <from position="{$firstPos}" xml:id="{$first/@xml:id}"><xsl:value-of select="$baseUrlOld"/><xsl:value-of select="$workID"/>_[<xsl:value-of select="$firstPos"/>-<xsl:value-of select="$lastPos"/>]</from>
                    <xsl:text> -> </xsl:text>
                    <to  position="{$lastPos}" xml:id="{$last/@xml:id}"><xsl:value-of select="$baseUrlNew"/>/suche?seite=<xsl:value-of select="$workPrefix"/>_<xsl:value-of select="$partPrefix"/>[<xsl:value-of select="$firstNo"/>-<xsl:value-of select="$lastNo"/>]</to>
                    <xsl:text>&#10;</xsl:text>
                </prefix>
            </xsl:for-each-group>
        </work>
        
        
    </xsl:template>
    
    
    <xsl:template name="concOutput">
        <xsl:variable name="workPrefix" select="substring-before((//tei:pb)[1]/@xml:id,'_')"/>
        <xsl:variable name="workID" select="//tei:idno[@type='cr-xq-mets']"/>
        <xsl:variable name="toc" select="doc('editions/'||$workID||'_toc.xml')"/>
        
        <xsl:for-each select="//tei:pb">
            <xsl:variable name="pageFragmentURL"><xsl:value-of select="$baseUrlNew"/><xsl:text>/edition-raw/</xsl:text><xsl:value-of select="@xml:id"/><xsl:text>.html</xsl:text></xsl:variable>
            <xsl:variable name="pos" select="count(preceding::tei:pb)+1"/>
            <xsl:variable name="fragmentID" select="$workID||'_'||$pos"/>
            <xsl:variable name="fragmentInToC" select="$toc//sru:term[sru:value=$fragmentID]"/>
            <xsl:value-of select="$baseUrlOld"/><xsl:value-of select="$fragmentID"/>
            <xsl:text>&#9;</xsl:text>
            <xsl:value-of select="$baseUrlNew"/>suche?seite=<xsl:value-of select="@xml:id"/>
            <xsl:if test="$debug = 'true'">
                <xsl:text>&#9;</xsl:text>
                <xsl:value-of select="$pageFragmentURL"/>
                <xsl:text>&#9;</xsl:text>
                <xsl:value-of select="$fragmentInToC/sru:displayTerm"/>
                <xsl:text>&#9;</xsl:text>
                <xsl:choose>
                    <xsl:when test="doc-available($pageFragmentURL)">
                        <xsl:value-of select="doc($pageFragmentURL)//*:h1"/>
                         
                    </xsl:when>
                    <xsl:otherwise>ERROR</xsl:otherwise>
                </xsl:choose>
            </xsl:if>
            <xsl:text>&#10;</xsl:text>
        </xsl:for-each>
    </xsl:template>
</xsl:stylesheet>
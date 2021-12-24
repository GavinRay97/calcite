/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.rel.externalize

import org.apache.calcite.rel.RelNode

/**
 * Callback for a relational expression to dump in XML format.
 */
class RelXmlWriter(pw: PrintWriter, detailLevel: SqlExplainLevel) : RelWriterImpl(pw, detailLevel, true) {
    //~ Instance fields --------------------------------------------------------
    private val xmlOutput: XmlOutput
    var generic = true

    //~ Constructors -----------------------------------------------------------
    // TODO jvs 23-Dec-2005:  honor detail level.  The current inheritance
    // structure makes this difficult without duplication; need to factor
    // out the filtering of attributes before rendering.
    init {
        xmlOutput = XmlOutput(pw)
        xmlOutput.setGlob(true)
        xmlOutput.setCompact(false)
    }

    //~ Methods ----------------------------------------------------------------
    @Override
    protected override fun explain_(
        rel: RelNode,
        values: List<Pair<String, Object>>
    ) {
        if (generic) {
            explainGeneric(rel, values)
        } else {
            explainSpecific(rel, values)
        }
    }

    /**
     * Generates generic XML (sometimes called 'element-oriented XML'). Like
     * this:
     *
     * <blockquote>
     * `
     * <RelNode id="1" type="Join"><br></br>
     * &nbsp;&nbsp;<Property name="condition">EMP.DEPTNO =
     * DEPT.DEPTNO</Property><br></br>
     * &nbsp;&nbsp;<Inputs><br></br>
     * &nbsp;&nbsp;&nbsp;&nbsp;<RelNode id="2" type="Project"><br></br>
     * &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;<Property name="expr1">x +
     * y</Property><br></br>
     * &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;<Property
     * name="expr2">45</Property><br></br>
     * &nbsp;&nbsp;&nbsp;&nbsp;</RelNode><br></br>
     * &nbsp;&nbsp;&nbsp;&nbsp;<RelNode id="3" type="TableAccess"><br></br>
     * &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;<Property
     * name="table">SALES.EMP</Property><br></br>
     * &nbsp;&nbsp;&nbsp;&nbsp;</RelNode><br></br>
     * &nbsp;&nbsp;</Inputs><br></br>
     * </RelNode>`
    </blockquote> *
     *
     * @param rel    Relational expression
     * @param values List of term-value pairs
     */
    private fun explainGeneric(
        rel: RelNode,
        values: List<Pair<String, Object>>
    ) {
        val relType: String = rel.getRelTypeName()
        xmlOutput.beginBeginTag("RelNode")
        xmlOutput.attribute("type", relType)
        xmlOutput.endBeginTag("RelNode")
        val inputs: List<RelNode> = ArrayList()
        for (pair in values) {
            if (pair.right is RelNode) {
                inputs.add(pair.right as RelNode)
                continue
            }
            if (pair.right == null) {
                continue
            }
            xmlOutput.beginBeginTag("Property")
            xmlOutput.attribute("name", pair.left)
            xmlOutput.endBeginTag("Property")
            xmlOutput.cdata(pair.right.toString())
            xmlOutput.endTag("Property")
        }
        xmlOutput.beginTag("Inputs", null)
        spacer.add(2)
        for (input in inputs) {
            input.explain(this)
        }
        spacer.subtract(2)
        xmlOutput.endTag("Inputs")
        xmlOutput.endTag("RelNode")
    }

    /**
     * Generates specific XML (sometimes called 'attribute-oriented XML'). Like
     * this:
     *
     * <blockquote><pre>
     * &lt;Join condition="EMP.DEPTNO = DEPT.DEPTNO"&gt;
     * &lt;Project expr1="x + y" expr2="42"&gt;
     * &lt;TableAccess table="SALES.EMPS"&gt;
     * &lt;/Join&gt;
    </pre></blockquote> *
     *
     * @param rel    Relational expression
     * @param values List of term-value pairs
     */
    private fun explainSpecific(
        rel: RelNode,
        values: List<Pair<String, Object>>
    ) {
        val tagName: String = rel.getRelTypeName()
        xmlOutput.beginBeginTag(tagName)
        xmlOutput.attribute("id", rel.getId() + "")
        for (value in values) {
            if (value.right is RelNode) {
                continue
            }
            xmlOutput.attribute(
                value.left,
                Objects.toString(value.right)
            )
        }
        xmlOutput.endBeginTag(tagName)
        spacer.add(2)
        for (input in rel.getInputs()) {
            input.explain(this)
        }
        spacer.subtract(2)
    }
}

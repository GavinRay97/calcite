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
package org.apache.calcite.util

import kotlin.Throws
import org.apache.calcite.util.NameSet

/**
 * A collection of terms.
 *
 *
 * (This is not a real class. It is here so that terms which do not map to
 * classes can be referenced in Javadoc.)
 */
interface Glossary {
    companion object {
        //~ Static fields/initializers ---------------------------------------------
        // CHECKSTYLE: OFF
        /**
         *
         * This table shows how and where the Gang of Four patterns are applied.
         * The table uses information from the GoF book and from a course on
         * advanced object design taught by Craig Larman.
         *
         *
         * The patterns are in three groups depicting frequency of use. The
         * patterns in light green are used
         * *frequently*. Those in yellow have
         * *moderate* use. Patterns in red are
         * *infrequently* used. The GoF column gives the original Gang Of Four
         * category for the pattern. The Problem and Pattern columns are from
         * Craig's refinement of the type of problems they apply to and a refinement
         * of the original three pattern categories.
         *
         * <table style="border-spacing:0px;padding:3px;border:1px">
         * <caption>
         * [
 * **Gang of Four Patterns**](http://www.onr.com/user/loeffler/java/references.html#gof)
        </caption> *
         * <tr>
         *
         *
         * <th>Pattern Name</th>
         * <th style="text-align:middle">[GOF Category](#category)</th>
         * <th style="text-align:middle">Problem</th>
         * <th style="text-align:middle">Pattern</th>
         * <th style="text-align:middle">Often Uses</th>
         * <th style="text-align:middle">Related To</th>
        </tr> *
         *
         *
         * <tr>
         * <td style="background-color:lime">[Abstract Factory](#AbstractFactoryPattern)
        </td> *
         * <td style="background-color:teal;color:white">Creational</td>
         * <td>Creating Instances</td>
         * <td>Class/Interface Definition plus Inheritance</td>
         * <td>[Factory Method](#FactoryMethodPattern)<br></br>
         * [Prototype](#PrototypePattern)<br></br>
         * [Singleton](#SingletonPattern) with [
 * Facade](#FacadePattern)</td>
         * <td>[Factory Method](#FactoryMethodPattern)<br></br>
         * [Prototype](#PrototypePattern)<br></br>
         * [Singleton](#SingletonPattern)</td>
        </tr> *
         *
         * <tr>
         * <td style="background-color:lime">[Object Adapter](#ObjectAdapterPattern)
        </td> *
         * <td style="background-color:silver">Structural</td>
         * <td>Interface</td>
         * <td>Wrap One</td>
         * <td style="text-align:middle">-</td>
         * <td>[Bridge](#BridgePattern)<br></br>
         * [Decorator](#DecoratorPattern)<br></br>
         * [Proxy](#ProxyPattern)</td>
        </tr> *
         * <tr>
         * <td style="background-color:lime">[Command](#CommandPattern)</td>
         * <td style="background-color:maroon;color:white">Behavioral</td>
         * <td>Organization or Communication of Work<br></br>
         * Action/Response</td>
         * <td>Behavior Objects</td>
         * <td>[Composite](#CompositePattern)</td>
         * <td>[Composite](#CompositePattern)<br></br>
         * [Memento](#MementoPattern)<br></br>
         * [Prototype](#PrototypePattern)</td>
        </tr> *
         * <tr>
         * <td style="background-color:lime">[Composite](#CompositePattern)</td>
         * <td style="background-color:silver">Structural</td>
         * <td>Structural Decomposition of Objects or Subsystems</td>
         * <td>Wrap Many</td>
         * <td style="text-align:middle">-</td>
         * <td>[Decorator](#DecoratorPattern)<br></br>
         * [Iterator](#IteratorPattern)<br></br>
         * [Visitor](#VisitorPattern)</td>
        </tr> *
         * <tr>
         * <td style="background-color:lime">[Decorator](#DecoratorPattern)</td>
         * <td style="background-color:silver">Structural</td>
         * <td>Instance Behavior</td>
         * <td>Wrap One</td>
         * <td style="text-align:middle">-</td>
         * <td>[Object Adapter](#ObjectAdapterPattern)<br></br>
         * [Composite](#CompositePattern)<br></br>
         * [Strategy](#StrategyPattern)</td>
        </tr> *
         * <tr>
         * <td style="background-color:lime">[Facade](#FacadePattern)</td>
         * <td style="background-color:silver">Structural</td>
         * <td>Access Control<br></br>
         * &nbsp;
         * <hr></hr>
         *
         * Structural Decomposition of Objects or Subsystems</td>
         * <td>Wrap Many</td>
         * <td>[Singleton](#SingletonPattern) with [Abstract Factory](#AbstractFactoryPattern)</td>
         * <td>[Abstract Factory](#AbstractFactoryPattern)<br></br>
         * [Mediator](#MediatorPattern)</td>
        </tr> *
         * <tr>
         * <td style="background-color:lime">[Flyweight](#FlyweightPattern)</td>
         * <td style="background-color:silver">Structural</td>
         * <td>Shared Resource Handling</td>
         * <td>Object State or Values</td>
         * <td style="text-align:middle">-</td>
         * <td>[Singleton](#SingletonPattern)<br></br>
         * [State](#StatePattern)<br></br>
         * [Strategy](#StrategyPattern)<br></br>
         * Shareable</td>
        </tr> *
         * <tr>
         * <td style="background-color:lime">[Iterator](#IteratorPattern)</td>
         * <td style="background-color:maroon;color:white">Behavioral</td>
         * <td>Traversal Algorithm<br></br>
         * &nbsp;
         * <hr></hr>
         *
         * Access Control</td>
         * <td>Low Coupling</td>
         * <td style="text-align:middle">-</td>
         * <td>[Composite](#CompositePattern)<br></br>
         * [Factory Method](#FactoryMethodPattern)<br></br>
         * [Memento](#MementoPattern)</td>
        </tr> *
         * <tr>
         * <td style="background-color:lime">[Observer](#ObserverPattern)</td>
         * <td style="background-color:maroon;color:white">Behavioral</td>
         * <td>Event Response<br></br>
         * &nbsp;
         * <hr></hr>
         *
         * Organization or Communication of Work</td>
         * <td>Low Coupling</td>
         * <td style="text-align:middle">-</td>
         * <td>[Mediator](#MediatorPattern)<br></br>
         * [Singleton](#SingletonPattern)</td>
        </tr> *
         * <tr>
         * <td style="background-color:lime">[Proxy](#ProxyPattern)</td>
         * <td style="background-color:silver">Structural</td>
         * <td>Access Control</td>
         * <td>Wrap One</td>
         * <td style="text-align:middle">-</td>
         * <td>[Adapter](#ObjectAdapterPattern)<br></br>
         * [Decorator](#DecoratorPattern)</td>
        </tr> *
         * <tr>
         * <td style="background-color:lime">[Singleton](#SingletonPattern)</td>
         * <td style="background-color:teal;color:white">Creational</td>
         * <td>Access Control</td>
         * <td>Other</td>
         * <td style="text-align:middle">-</td>
         * <td>[Abstract Factory](#AbstractFactoryPattern)<br></br>
         * [Builder](#BuilderPattern)<br></br>
         * [Prototype](#PrototypePattern)</td>
        </tr> *
         * <tr>
         * <td style="background-color:lime">[State](#StatePattern)</td>
         * <td style="background-color:maroon;color:white">Behavioral</td>
         * <td>Instance Behavior</td>
         * <td>Object State or Values</td>
         * <td>[Flyweight](#FlyweightPattern)</td>
         * <td>[Flyweight](#FlyweightPattern)<br></br>
         * [Singleton](#SingletonPattern)</td>
        </tr> *
         * <tr>
         * <td style="background-color:lime">[Strategy](#StrategyPattern)</td>
         * <td style="background-color:maroon;color:white">Behavioral</td>
         * <td>Single Algorithm</td>
         * <td>Behavior Objects</td>
         * <td style="text-align:middle">-</td>
         * <td>[Flyweight](#FlyweightPattern)<br></br>
         * [State](#StatePattern)<br></br>
         * [Template Method](#TemplateMethodPattern)</td>
        </tr> *
         * <tr>
         * <td style="background-color:lime">[Template Method](#TemplateMethodPattern)
        </td> *
         * <td style="background-color:maroon;color:white">Behavioral</td>
         * <td>Single Algorithm</td>
         * <td>Class or Interface Definition plus Inheritance</td>
         * <td style="text-align:middle">-</td>
         * <td>[Strategy](#StrategyPattern)</td>
        </tr> *
         *
         * <tr>
         * <td style="background-color:yellow">[Class Adapter](#ClassAdapterPattern)
        </td> *
         * <td style="background-color:silver">Structural</td>
         * <td>Interface</td>
         * <td>Class or Interface Definition plus Inheritance</td>
         * <td style="text-align:middle">-</td>
         * <td>[Bridge](#BridgePattern)<br></br>
         * [Decorator](#DecoratorPattern)<br></br>
         * [Proxy](#ProxyPattern)</td>
        </tr> *
         * <tr>
         * <td style="background-color:yellow">[Bridge](#BridgePattern)</td>
         * <td style="background-color:silver">Structural</td>
         * <td>Implementation</td>
         * <td>Wrap One</td>
         * <td style="text-align:middle">-</td>
         * <td>[Abstract Factory](#AbstractFactoryPattern)<br></br>
         * [Class Adaptor](#ClassAdapterPattern)</td>
        </tr> *
         * <tr>
         * <td style="background-color:yellow">[Builder](#BuilderPattern)</td>
         * <td style="background-color:teal;color:white">Creational</td>
         * <td>Creating Structures</td>
         * <td>Class or Interface Definition plus Inheritance</td>
         * <td style="text-align:middle">-</td>
         * <td>[Abstract Factory](#AbstractFactoryPattern)<br></br>
         * [Composite](#CompositePattern)</td>
        </tr> *
         * <tr>
         * <td style="background-color:yellow">[Chain of
 * Responsibility](#ChainOfResponsibilityPattern)</td>
         * <td style="background-color:maroon;color:white">Behavioral</td>
         * <td>Single Algorithm<br></br>
         * &nbsp;
         * <hr></hr>
         *
         * Organization or Communication of Work</td>
         * <td>Low Coupling</td>
         * <td style="text-align:middle">-</td>
         * <td>[Composite](#CompositePattern)</td>
        </tr> *
         * <tr>
         * <td style="background-color:yellow">[Factory Method](#FactoryMethodPattern)
        </td> *
         * <td style="background-color:teal;color:white">Creational</td>
         * <td>Creating Instances</td>
         * <td>Class or Interface Definition plus Inheritance</td>
         * <td>[Template Method](#TemplateMethodPattern)</td>
         * <td>[Abstract Factory](#AbstractFactoryPattern)<br></br>
         * [Template Method](#TemplateMethodPattern)<br></br>
         * [Prototype](#PrototypePattern)</td>
        </tr> *
         * <tr>
         * <td style="background-color:yellow">[Mediator](#MediatorPattern)</td>
         * <td style="background-color:maroon;color:white">Behavioral</td>
         * <td>Interaction between Objects<br></br>
         * &nbsp;
         * <hr></hr>
         *
         * Organization or Communication of Work</td>
         * <td>Low Coupling</td>
         * <td style="text-align:middle">-</td>
         * <td>[Facade](#FacadePattern)<br></br>
         * [Observer](#ObserverPattern)</td>
        </tr> *
         * <tr>
         * <td style="background-color:yellow">[Prototype](#PrototypePattern)</td>
         * <td style="background-color:teal;color:white">Creational</td>
         * <td>Creating Instances</td>
         * <td>Other</td>
         * <td style="text-align:middle">-</td>
         * <td>[Prototype](#PrototypePattern)<br></br>
         * [Composite](#CompositePattern)<br></br>
         * [Decorator](#DecoratorPattern)</td>
        </tr> *
         * <tr>
         * <td style="background-color:yellow">[Visitor](#VisitorPattern)</td>
         * <td style="background-color:maroon;color:white">Behavioral</td>
         * <td>Single Algorithm</td>
         * <td>Behavior Objects</td>
         * <td style="text-align:middle">-</td>
         * <td>[Composite](#CompositePattern)<br></br>
         * [Visitor](#VisitorPattern)</td>
        </tr> *
         *
         * <tr>
         * <td style="background-color:red;color:white">[
 * Interpreter](#InterpreterPattern)</td>
         * <td style="background-color:maroon;color:white">Behavioral</td>
         * <td>Organization or Communication of Work</td>
         * <td>Other</td>
         * <td style="text-align:middle">-</td>
         * <td>[Composite](#CompositePattern)<br></br>
         * [Flyweight](#FlyweightPattern)<br></br>
         * [Iterator](#IteratorPattern)<br></br>
         * [Visitor](#VisitorPattern)</td>
        </tr> *
         * <tr>
         * <td style="background-color:red;color:white">[
 * Memento](#MementoPattern)</td>
         * <td style="background-color:maroon;color:white">Behavioral</td>
         * <td>Instance Management</td>
         * <td>Object State or Values</td>
         * <td style="text-align:middle">-</td>
         * <td>[Command](#CommandPattern)<br></br>
         * [Iterator](#IteratorPattern)</td>
        </tr> *
        </table> *
         */
        // CHECKSTYLE: ON
        @Nullable
        val PATTERN: Glossary? = null

        /**
         * Provide an interface for creating families of related or dependent
         * objects without specifying their concrete classes. (See [GoF](http://c2.com/cgi/wiki?AbstractFactoryPattern).)
         */
        @Nullable
        val ABSTRACT_FACTORY_PATTERN: Glossary? = null

        /**
         * Separate the construction of a complex object from its representation so
         * that the same construction process can create different representations.
         * (See [GoF](http://c2.com/cgi/wiki?BuilderPattern).)
         */
        @Nullable
        val BUILDER_PATTERN: Glossary? = null

        /**
         * Define an interface for creating an object, but let subclasses decide
         * which class to instantiate. Lets a class defer instantiation to
         * subclasses. (See [
 * GoF](http://c2.com/cgi/wiki?FactoryMethodPattern).)
         */
        @Nullable
        val FACTORY_METHOD_PATTERN: Glossary? = null

        /**
         * Specify the kinds of objects to create using a prototypical instance, and
         * create new objects by copying this prototype. (See [GoF](http://c2.com/cgi/wiki?PrototypePattern).)
         */
        @Nullable
        val PROTOTYPE_PATTERN: Glossary? = null

        /**
         * Ensure a class only has one instance, and provide a global point of
         * access to it. (See [
 * GoF](http://c2.com/cgi/wiki?SingletonPattern).)
         *
         *
         * Note that a common way of implementing a singleton, the so-called [
 * double-checked locking pattern](http://www.cs.umd.edu/~pugh/java/memoryModel/DoubleCheckedLocking.html), is fatally flawed in Java. Don't use
         * it!
         */
        @Nullable
        val SINGLETON_PATTERN: Glossary? = null

        /**
         * Convert the interface of a class into another interface clients expect.
         * Lets classes work together that couldn't otherwise because of
         * incompatible interfaces. (See [GoF](http://c2.com/cgi/wiki?AdapterPattern).)
         */
        @Nullable
        val ADAPTER_PATTERN: Glossary? = null

        /**
         * Decouple an abstraction from its implementation so that the two can very
         * independently. (See [
 * GoF](http://c2.com/cgi/wiki?BridgePattern).)
         */
        @Nullable
        val BRIDGE_PATTERN: Glossary? = null

        /**
         * Compose objects into tree structures to represent part-whole hierarchies.
         * Lets clients treat individual objects and compositions of objects
         * uniformly. (See [
 * GoF](http://c2.com/cgi/wiki?CompositePattern).)
         */
        @Nullable
        val COMPOSITE_PATTERN: Glossary? = null

        /**
         * Attach additional responsibilities to an object dynamically. Provides a
         * flexible alternative to subclassing for extending functionality. (See [GoF](http://c2.com/cgi/wiki?DecoratorPattern).)
         */
        @Nullable
        val DECORATOR_PATTERN: Glossary? = null

        /**
         * Provide a unified interface to a set of interfaces in a subsystem.
         * Defines a higher-level interface that makes the subsystem easier to use.
         * (See [GoF](http://c2.com/cgi/wiki?FacadePattern).)
         */
        @Nullable
        val FACADE_PATTERN: Glossary? = null

        /**
         * Use sharing to support large numbers of fine-grained objects efficiently.
         * (See [GoF](http://c2.com/cgi/wiki?FlyweightPattern).)
         */
        @Nullable
        val FLYWEIGHT_PATTERN: Glossary? = null

        /**
         * Provide a surrogate or placeholder for another object to control access
         * to it. (See [GoF](http://c2.com/cgi/wiki?ProxyPattern).)
         */
        @Nullable
        val PROXY_PATTERN: Glossary? = null

        /**
         * Avoid coupling the sender of a request to its receiver by giving more
         * than one object a chance to handle the request. Chain the receiving
         * objects and pass the request along the chain until an object handles it.
         * (See [
 * GoF](http://c2.com/cgi/wiki?ChainOfResponsibilityPattern).)
         */
        @Nullable
        val CHAIN_OF_RESPONSIBILITY_PATTERN: Glossary? = null

        /**
         * Encapsulate a request as an object, thereby letting you parameterize
         * clients with different requests, queue or log requests, and support
         * undoable operations. (See [GoF](http://c2.com/cgi/wiki?CommandPattern).)
         */
        @Nullable
        val COMMAND_PATTERN: Glossary? = null

        /**
         * Given a language, define a representation for its grammar along with an
         * interpreter that uses the representation to interpret sentences in the
         * language. (See [
 * GoF](http://c2.com/cgi/wiki?InterpreterPattern).)
         */
        @Nullable
        val INTERPRETER_PATTERN: Glossary? = null

        /**
         * Provide a way to access the elements of an aggregate object sequentially
         * without exposing its underlying representation. (See [GoF](http://c2.com/cgi/wiki?IteratorPattern).)
         */
        @Nullable
        val ITERATOR_PATTERN: Glossary? = null

        /**
         * Define an object that encapsulates how a set of objects interact.
         * Promotes loose coupling by keeping objects from referring to each other
         * explicitly, and it lets you vary their interaction independently. (See [GoF](http://c2.com/cgi/wiki?MediatorPattern).)
         */
        @Nullable
        val MEDIATOR_PATTERN: Glossary? = null

        /**
         * Without violating encapsulation, capture and externalize an objects's
         * internal state so that the object can be restored to this state later.
         * (See [GoF](http://c2.com/cgi/wiki?MementoPattern).)
         */
        @Nullable
        val MEMENTO_PATTERN: Glossary? = null

        /**
         * Define a one-to-many dependency between objects so that when one object
         * changes state, all its dependents are notified and updated automatically.
         * (See [GoF](http://c2.com/cgi/wiki?ObserverPattern).)
         */
        @Nullable
        val OBSERVER_PATTERN: Glossary? = null

        /**
         * Allow an object to alter its behavior when its internal state changes.
         * The object will appear to change its class. (See [GoF](http://c2.com/cgi/wiki?StatePattern).)
         */
        @Nullable
        val STATE_PATTERN: Glossary? = null

        /**
         * Define a family of algorithms, encapsulate each one, and make them
         * interchangeable. Lets the algorithm vary independently from clients that
         * use it. (See [GoF](http://c2.com/cgi/wiki?StrategyPattern).)
         */
        @Nullable
        val STRATEGY_PATTERN: Glossary? = null

        /**
         * Define the skeleton of an algorithm in an operation, deferring some steps
         * to subclasses. Lets subclasses redefine certain steps of an algorithm
         * without changing the algorithm's structure. (See [GoF](http://c2.com/cgi/wiki?TemplateMethodPattern).)
         */
        @Nullable
        val TEMPLATE_METHOD_PATTERN: Glossary? = null

        /**
         * Represent an operation to be performed on the elements of an object
         * structure. Lets you define a new operation without changing the classes
         * of the elements on which it operates. (See [GoF](http://c2.com/cgi/wiki?VisitorPattern).)
         */
        @Nullable
        val VISITOR_PATTERN: Glossary? = null

        /**
         * The official SQL-92 standard (ISO/IEC 9075:1992). To reference this
         * standard from methods that implement its rules, use the &#64;sql.92
         * custom block tag in Javadoc comments; for the tag body, use the format
         * `<SectionId> [ ItemType <ItemId> ]`, where
         *
         *
         *  * `SectionId` is the numbered or named section in the table
         * of contents, e.g. "Section 4.18.9" or "Annex A"
         *  * `ItemType` is one of { Table, Syntax Rule, Access Rule,
         * General Rule, or Leveling Rule }
         *  * `ItemId` is a dotted path expression to the specific item
         *
         *
         *
         * For example,
         *
         * <blockquote><pre>`&#64;sql.92 Section 11.4 Syntax Rule 7.c
        `</pre></blockquote> *
         *
         *
         * is a well-formed reference to the rule for the default character set to
         * use for column definitions of character type.
         *
         *
         * Note that this tag is a block tag (like &#64;see) and cannot be used
         * inline.
         */
        @Nullable
        val SQL92: Glossary? = null

        /**
         * The official SQL:1999 standard (ISO/IEC 9075:1999), which is broken up
         * into five parts. To reference this standard from methods that implement
         * its rules, use the &#64;sql.99 custom block tag in Javadoc comments; for
         * the tag body, use the format `<PartId> <SectionId> [
         * ItemType <ItemId> ]`, where
         *
         *
         *  * `PartId` is the numbered part (up to Part 5)
         *  * `SectionId` is the numbered or named section in the part's
         * table of contents, e.g. "Section 4.18.9" or "Annex A"
         *  * `ItemType` is one of { Table, Syntax Rule, Access Rule,
         * General Rule, or Conformance Rule }
         *  * `ItemId` is a dotted path expression to the specific item
         *
         *
         *
         * For example,
         *
         * <blockquote><pre>`&#64;sql.99 Part 2 Section 11.4 Syntax Rule 7.b
        `</pre></blockquote> *
         *
         *
         * is a well-formed reference to the rule for the default character set to
         * use for column definitions of character type.
         *
         *
         * Note that this tag is a block tag (like &#64;see) and cannot be used
         * inline.
         */
        @Nullable
        val SQL99: Glossary? = null

        /**
         * The official SQL:2003 standard (ISO/IEC 9075:2003), which is broken up
         * into numerous parts. To reference this standard from methods that
         * implement its rules, use the &#64;sql.2003 custom block tag in Javadoc
         * comments; for the tag body, use the format `<PartId>
         * <SectionId> [ ItemType <ItemId> ]`, where
         *
         *
         *  * `PartId` is the numbered part
         *  * `SectionId` is the numbered or named section in the part's
         * table of contents, e.g. "Section 4.11.2" or "Annex A"
         *  * `ItemType` is one of { Table, Syntax Rule, Access Rule,
         * General Rule, or Conformance Rule }
         *  * `ItemId` is a dotted path expression to the specific item
         *
         *
         *
         * For example,
         *
         * <blockquote><pre>`&#64;sql.2003 Part 2 Section 11.4 Syntax Rule 10.b
        `</pre></blockquote> *
         *
         *
         * is a well-formed reference to the rule for the default character set to
         * use for column definitions of character type.
         *
         *
         * Note that this tag is a block tag (like &#64;see) and cannot be used
         * inline.
         */
        @Nullable
        val SQL2003: Glossary? = null
    }
}

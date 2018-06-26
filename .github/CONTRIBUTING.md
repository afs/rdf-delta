RDF Delta : Contributing
==========================

The project welcomes contributions, large and small, from anyone.

The mailing list for project-wide discussions is users@jena.apache.org
and all development work happens in public, using that list.

## Contributions

Contributions can be made via github pull requests. See the
[legal section](#legal) below.

Contributions should include:

* Tests
* Documentation as needed

## Workflow

* On github, fork http://github.com/afs/rdf-delta into you github account.
* Create a branch in your fork for the contribution.
* Make your changes. Include the Apache license header at the top of
  each new file.
* Generate a pull request via github. Further changes to your branch will automatically
  show up in the pull request

### Code

Code style is about making the code clear for the next person who looks
at the code (which may be a future you!).

The project prefers code to be formatted in the common java style with
sensible deviation for short forms.

The project does not enforce a particular style but asks for:

* Kernighan and Ritchie style "Egyptian brackets" braces.
* Spaces for indentation
* No `@author` tags.
* One statement per line
* Indent level 4 for Java
* Indent level 2 for XML

See, for illustration:
https://google.github.io/styleguide/javaguide.html#s4-formatting

The code should have no warnings, in particular,
* use `@Override`
* use types for generics, 
* don't declared checked exceptions that are not used.
* Don't have unused imports

Use `@SuppressWarnings("unused")` as necessary.

## Legal

All contributions are understood to be made as contributions under the
framework below which is based on that used by the Apache Foundation in
the [Individual Contributor License Agreement (ICLA)](https://www.apache.org/licenses/icla.pdf).

The project cannot accept contributions with unclear ownership nor
contributions containing work by other people without a clear agreement
from those people.

The purpose of this agreement is to clearly define the terms under which
intellectual property has been contributed and to leave the path open to
migrating the code to the Apache Foundation.

#### Grant of Copyright License. 

You hereby grant to the Project and to recipients of software
distributed by the Project a perpetual, worldwide, non-exclusive,
no-charge, royalty-free, irrevocable copyright license to reproduce,
prepare derivative works of, publicly display, publicly perform,
sublicense, and distribute Your Contributions and such derivative works.

#### Grant of Patent License.

You hereby grant to the Project and to recipients of software
distributed by the Project a perpetual, worldwide, non-exclusive,
no-charge, royalty-free, irrevocable (except as stated in this section)
patent license to make, have made, use, offer to sell, sell, import, and
otherwise transfer the Work, where such license applies only to those
patent claims licensable by You that are necessarily infringed by Your
Contribution(s) alone or by combination of Your Contribution(s) with the
Work to which such Contribution(s) was submitted. If any entity
institutes patent litigation against You or any other entity (including
a cross-claim or counterclaim in a lawsuit) alleging that your
Contribution, or the Work to which you have contributed, constitutes
direct or contributory patent infringement, then any patent licenses
granted to that entity under this Agreement for that Contribution or
Work shall terminate as of the date such litigation is filed.

#### Entitled to Grant Copyright License and Patent License.

You represent that you are legally entitled to grant the above license
sections. If your employer(s) has rights to intellectual property that
you create that includes your Contributions, you represent that you have
received permission to make Contributions on behalf of that employer,
that your employer has waived such rights for your Contributions to the
Project, or that your employer has executed a separate arrangement with
the Project.

You represent that each of Your Contributions is Your original creation.
You represent that Your Contribution submissions include complete
details of any third-party license or other restriction (including, but
not limited to, related patents and trademarks) of which you are
personally aware and which are associated with any part of Your
Contributions.

You agree to notify the Project of any facts or circumstances of which
you become aware that would make these representations inaccurate in any
respect.

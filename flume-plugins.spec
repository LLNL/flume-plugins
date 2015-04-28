Name:           	flume-plugins
Version:        	1.0.1
Release:        	1%{?dist}
Summary:        	Apache Flume Plugins

Group:          	Development/Libraries
License:        	BSD
BuildRoot: 		%{_tmppath}/%{name}-%{version}
Source0:        	%{name}-%{version}.tar.gz
BuildArch:      	noarch

BuildRequires:  	java-devel >= 1:1.6.0
BuildRequires:  	flume_2_2_4_2_2
Requires:       	java >= 1:1.6.0
Requires:       	flume_2_2_4_2_2
Requires:       	hbase_2_2_0_0_2041

%define flumeplugindir  /usr/hdp/2.2.4.2-2/flume/plugins.d

%description
This package includes a collection of plugins for Apache Flume.

%prep
%setup -q

%build
make

%install
[ "%{buildroot}" != "/" ] && rm -rf %{buildroot}
mkdir -p $RPM_BUILD_ROOT%{flumeplugindir}
DESTDIR="$RPM_BUILD_ROOT" make install

%clean
rm -rf $RPM_BUILD_ROOT

%files
%defattr(644,root,root)
%{flumeplugindir}/*

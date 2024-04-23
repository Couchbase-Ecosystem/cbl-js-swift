#
# Be sure to run `pod lib lint CbliteSwiftJsLib.podspec' before submitting for review. 
#
# Any lines starting with a # are optional, but their use is encouraged
# To learn more about a Podspec see https://guides.cocoapods.org/syntax/podspec.html
#

Pod::Spec.new do |s|
  s.name             = 'CbliteSwiftJsLib'
  s.version          = '0.1.0'
  s.summary          = 'Couchbase Lite Swift libary for cblite.js (Javascript) Library'

# This description is used to generate tags and improve search results.
#   * Think: What does it do? Why did you write it? What is the focus?
#   * Try to keep it short, snappy and to the point.
#   * Write the description between the DESC delimiters below.
#   * Finally, don't worry about the indent, CocoaPods strips it!

  s.description      = <<-DESC
  Couchbase Lite Swift libary for cblite.js (Javascript) Library.  This is a set of shared code used by the Ionic and React Native plugins for Couchbase Lite.
                       DESC

  s.homepage         = 'https://github.com/Couchbase-Ecosystem/cbl-js-swift'
  # s.screenshots     = 'www.example.com/screenshots_1', 'www.example.com/screenshots_2'
  s.license          = { :type => 'Apache License, Version 2.0', :file => 'LICENSE' }
  s.author           = 'Couchbase'
  s.source           = { :git => 'https://github.com/Couchbase-Ecosystem/cbl-js-swift', :tag => s.version.to_s }
  s.social_media_url = ''

  s.ios.deployment_target = '13.0'
  s.swift_version = '5.5'

  s.source_files = 'CbliteSwiftJsLib/Classes/**/*'
  s.dependency 'CouchbaseLite-Swift-Enterprise', '~> 3.1'
end

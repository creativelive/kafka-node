'use strict';

var util = require('util');
var _ = require('lodash');
var HighLevelConsumer = require('./highLevelConsumer.js');

var STATE_DONE = 'done';
var STATE_SUSPENDED = 'suspended';
var STATE_DEFAULT = 'default';

var SuspendableConsumer = function(client, topics, options) {
  this.suspended = false;
  this.resumeAfterDone = false;
  this.previousOffsets = {}; // previously-seen offsets before suspension
  HighLevelConsumer.call(this, client, topics, options);
};

util.inherits(SuspendableConsumer, HighLevelConsumer);

SuspendableConsumer.prototype.connect = function() {
  var self = this;
  this.setPartitionStates(STATE_DEFAULT);
  this.on('message', this.onMessageEvent);
  SuspendableConsumer.super_.prototype.connect.call(self);
};

SuspendableConsumer.prototype.onMessageEvent = function (message) {
  if (!this.suspended) {
    //console.log('==== updating previous offsets:', message.topic, message.partition, message.offset);
    this.setPreviousOffset(message.topic, message.partition, message.offset);
    this.emit('data', message);
  }
};

SuspendableConsumer.prototype.onDone = function(topics) {
  var self = this;
  if (this.suspended) {
    Object.keys(topics).forEach(function(topic) {
      Object.keys(topics[topic]).forEach(function(partition) {
        self.setPartitionState(topic, partition, STATE_DONE);
      })
    });
    if (this.resumeAfterDone) {
      this.resume();
    }
  } else {
    SuspendableConsumer.super_.prototype.onDone.call(self, topics);
  }
};

SuspendableConsumer.prototype.updateOffsets = function(topics, initing) {
  if (!this.suspended) {
    SuspendableConsumer.super_.prototype.updateOffsets.call(this, topics, initing);
  }
};

SuspendableConsumer.prototype.setPreviousOffset = function(topic, partition, offset) {
  if (!topic || typeof partition !== 'number' || typeof offset !== 'number') {
    console.warn('[WARN] invalid args when setting offset', topic, partition, offset);
    return;
  }
  if (!this.previousOffsets[topic]) {
    this.previousOffsets[topic] = {};
  }
  this.previousOffsets[topic][partition] = offset;
};

SuspendableConsumer.prototype.setPartitionStates = function(state) {
  _.each(this.topicPayloads, function(payload) {
    payload.state = state;
  });
};

SuspendableConsumer.prototype.setPartitionState = function(topic, partition, state) {
  for (var i = 0; i < this.topicPayloads.length; i++) {
    var payload = this.topicPayloads[i];
    if (payload && payload.topic === topic && payload.partition === partition) {
      payload.state = state;
      break;
    }
  }
};

SuspendableConsumer.prototype.resumeFromPreviousOffsets = function() {
  var self = this;

  console.log('******** resuming *********');
  _.each(self.topicPayloads, function(p) {
    console.log('    partition:', p.partition, 'offset:', p.offset);
  });
  console.log('  previous offsets:', self.previousOffsets);


  Object.keys(self.previousOffsets).forEach(function(topic) {
    Object.keys(self.previousOffsets[topic]).forEach(function(partition) {
      SuspendableConsumer.super_.prototype.setOffset.call(self, topic, partition, self.previousOffsets[topic][partition] + 1);
    });
  });
  self.previousOffsets = {};
  setImmediate(function() {
    self.fetch();
  });
};

SuspendableConsumer.prototype.suspend = function() {
  this.suspended = true;
  this.setPartitionStates(STATE_SUSPENDED);
  console.log('````````` offsets:', this.previousOffsets);
};

SuspendableConsumer.prototype.resume = function() {
  if (this.suspended) {
    if (this.canResumeAllPartitions()) {
      console.log('can resume all');
      this.suspended = false;
      this.resumeAfterDone = false;
      this.setPartitionStates(STATE_DEFAULT);
      this.resumeFromPreviousOffsets();
    } else {
      console.log('resume after done');
      // wait for every partition to be done before resuming
      this.resumeAfterDone = true;
      setTimeout(this.resume, 5000);
    }
  } else if (this.paused) {
    SuspendableConsumer.super_.prototype.resume.call(this);
  }
};

SuspendableConsumer.prototype.canResumeAllPartitions = function() {
  for (var i = 0; i < this.topicPayloads.length; i++) {
    var payload = this.topicPayloads[i];
    if (payload.state && payload.state != STATE_DONE) {
      return false;
    }
  }
  return true;
};

SuspendableConsumer.prototype.commitProcessedOffsets = function(topic, partitionToOffsets) {
  if (this.options.autoCommit) {
    // ignore special commit request if autocommit is already turned on
    return;
  }
  var partitions = Object.keys(partitionToOffsets);
  var commits = this.topicPayloads.filter(function (p) {
    return /** p.offset !== 0 && **/ p.topic === topic && partitions.indexOf(p.partition) > -1;
  });
  _.each(commits, function(commit) {
    commit.offset = partitionToOffsets[commit.partition];
  });
  if (commits.length) {
    console.log('committing:', commits);
    this.client.sendOffsetCommitRequest(this.options.groupId, commits, function(err) {
      if (err) {
        console.log('[ERROR] when commiting processed offsets:', commits);
      }
    });
  } else {
    console.log('empty commits:', topic, partitionToOffsets, this.topicPayloads);
  }
};

module.exports = SuspendableConsumer;
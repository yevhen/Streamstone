import clsx from 'clsx';
import Heading from '@theme/Heading';
import styles from './styles.module.css';

const logo = require('@site/static/img/Logo.png').default;

const FeatureList = [
  {
    title: 'ACID Compliance',
    icon: '🛡️',
    description: (
      <>Fully ACID-compliant event storage for reliable, consistent operations.</>
    ),
  },
  {
    title: 'Optimistic Concurrency',
    icon: '🔄',
    description: (
      <>Built-in optimistic concurrency control to prevent conflicts and ensure data integrity.</>
    ),
  },
  {
    title: 'Duplicate Event Detection',
    icon: '🚦',
    description: (
      <>Automatic detection and prevention of duplicate events based on identity.</>
    ),
  },
  {
    title: 'Automatic Continuation',
    icon: '⏩',
    description: (
      <>Seamless handling of Azure Table Storage batch limits for both reads and writes.</>
    ),
  },
  {
    title: 'Custom Properties',
    icon: '🏷️',
    description: (
      <>Support for custom stream and event properties that you can query on.</>
    ),
  },
  {
    title: 'Inline Projections & Snapshots',
    icon: '📸',
    description: (
      <>Synchronous projections and snapshots for fast, up-to-date views.</>
    ),
  },
  {
    title: 'Change Tracking',
    icon: '🔍',
    description: (
      <>Change tracking support for inline projections and state management.</>
    ),
  },
  {
    title: 'Multi-Tenant Friendly',
    icon: '🏢',
    description: (
      <>Designed for multi-tenant architectures and scalable SaaS solutions.</>
    ),
  },
  {
    title: 'Sharding Support',
    icon: '🧩',
    description: (
      <>Jump consistent hashing for efficient sharding and horizontal scalability.</>
    ),
  },
];

function Feature({icon, title, description}) {
  return (
    <div className={clsx('col col--4')}>
      <div className="text--center" style={{fontSize: 48, marginBottom: 12}}>
        {icon}
      </div>
      <div className="text--center padding-horiz--md">
        <Heading as="h3">{title}</Heading>
        <p>{description}</p>
      </div>
    </div>
  );
}

export default function HomepageFeatures() {
  return (
    <section className={styles.features}>
      <div className="container">
        <div className="row">
          {FeatureList.map((props, idx) => (
            <Feature key={idx} {...props} />
          ))}
        </div>
      </div>
    </section>
  );
}
